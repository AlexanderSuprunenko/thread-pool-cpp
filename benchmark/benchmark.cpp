#include <thread_pool.hpp>

#ifdef WITH_ASIO
#include <asio_thread_pool.hpp>
#endif

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <future>

#include <cassert>
#include <memory>
#include <deque>
#include <sstream>

#include <sys/eventfd.h>
#include <unistd.h>

using namespace tp;

static const size_t CONCURRENCY = 16;
static const size_t REPOST_COUNT = 1000000;

///////
//time measurement helpers
using ticks_t = std::chrono::high_resolution_clock::duration;
using ticks_p = decltype(std::chrono::high_resolution_clock::now());
#define ticks_now() (std::chrono::high_resolution_clock::now())
using fp_ms = std::chrono::duration<double, std::milli>;

///////
//example of non-blocking queue synchronized with help of eventfd
template <typename T>
class MPMCBoundedQueueWithEventFD : protected MPMCBoundedQueue<T>
{
	int m_efd;
	void notify()
	{
		int64_t cnt = 1;
		write(m_efd, &cnt, sizeof(cnt));
	}

public:
	explicit MPMCBoundedQueueWithEventFD(size_t size)
		: MPMCBoundedQueue<T>(size)
		, m_efd( eventfd(0, 0) )
	{

	}

	MPMCBoundedQueueWithEventFD(MPMCBoundedQueueWithEventFD&& rhs) noexcept
	{
		*this = std::move(rhs);
	}

	MPMCBoundedQueueWithEventFD& operator=(MPMCBoundedQueueWithEventFD&& rhs) noexcept
	{
		if (this != &rhs)
		{
			static_cast<MPMCBoundedQueue<T>*>(this)->operator =(rhs);
			m_efd = std::move(rhs.m_efd);
		}
		return *this;
	}

	template <typename U>
	bool push(U&& data)
	{
		bool res = static_cast<MPMCBoundedQueue<T>*>(this)->push(data);
		notify();
		return res;
	}

	decltype(m_efd) getEFD() const
	{
		return m_efd;
	}

	bool pop(T& data)
	{
		return static_cast<MPMCBoundedQueue<T>*>(this)->pop(data);
	}
};

//test of MPMCBoundedQueueWithEventFD
void test_efd()
{
	MPMCBoundedQueueWithEventFD<std::string> que(512);
	{
		std::string s("my string");
		que.push(s);
	}
	que.push("my long long long long string string string string");
	{
		int efd = que.getEFD();
		int64_t cnt;
		read(efd, &cnt, sizeof(cnt));
		assert(cnt==2);
		std::string s;
		bool res = que.pop(s); assert(res);
		assert(s == "my string");
		res = que.pop(s); assert(res);
		assert(s == "my long long long long string string string string");
	}
}

////////
///
/// prototype of a job
///
template <typename Input, typename Output, typename Watcher>
class GraftJob
{
public:
	using ResQueue = MPMCBoundedQueue< GraftJob<Input, Output, Watcher> >;

	explicit GraftJob()
	{}

	explicit GraftJob(Input&& in, ResQueue* rq, Watcher* watcher) //, Output out)
		: m_in(in)
		, rq(rq)
		, watcher(watcher)
	{}

	GraftJob(GraftJob&& rhs) noexcept
	{
		*this = std::move(rhs);
	}


	GraftJob& operator=(GraftJob&& rhs) noexcept
	{
		if(this != &rhs)
		{
			m_in = std::move(rhs.m_in);
			m_out = std::move(rhs.m_out);
			rc = std::move(rhs.rc);
//			promise = std::move(rhs.promise);
			rq = std::move(rhs.rq);
			watcher = std::move(rhs.watcher);
		}
		return *this;
	}

	//main payload
	virtual void operator()()
	{
		auto begin_count = ticks_now();
		{
			m_out = m_in;
			while(std::next_permutation(m_out.begin(), m_out.end()))
			{
	//			std::cout << "x " << m_out << "\n";
			}
		}

		auto end_count = ticks_now();
		rc = end_count - begin_count;

//		promise.set_value();

		Watcher* save_watcher = watcher; //save watcher before move itself into resulting queue
		rq->push(std::move(*this)); //similar to "delete this;"
		save_watcher->notify();
	}

	Output getOutput() { return m_out; }
	ticks_t getReturnCode() { return rc; }

	virtual ~GraftJob() = default;
protected:
	Input m_in;
	Output m_out;

	ticks_t rc = ticks_t::zero();
	ResQueue* rq = nullptr;
	Watcher* watcher = nullptr;

//	std::promise<void> promise;

	char tmp_buf[1024]; //suppose polimorphic increase in size, so it is not good idea to move it from queue to queue,
	//but unique_ptr of it instead. see GJ_ptr.
};

class watcher;
using GJ = GraftJob<std::string, std::string, watcher>;

//////////////
/// \brief The GJ_ptr class
/// A wrapper of GraftJob that will be moved from queue to queue with fixed size.
/// It contains single data member of unique_ptr
///
class GJ_ptr final
{
	std::unique_ptr<GJ> ptr = nullptr;
public:
	GJ_ptr(GJ_ptr&& rhs)
	{
		*this = std::move(rhs);
	}
	GJ_ptr& operator = (GJ_ptr&& rhs)
	{
//		if(this != &rhs)
		{
			ptr = std::move(rhs.ptr);
		}
		return * this;
	}

	GJ_ptr() = delete;
	GJ_ptr(const GJ_ptr&) = delete;
	GJ_ptr& operator = (const GJ_ptr&) = delete;
	~GJ_ptr() = default;

	template<typename ...ARGS>
	GJ_ptr(ARGS&&... args) : ptr( new GJ( std::forward<ARGS>(args)...) )
	{
	}

	template<typename ...ARGS>
	void operator ()(ARGS... args)
	{
		ptr.get()->operator () (args...);
	}
};

/////////
/// \brief The watcher class. base class for all watchers
///
class watcher
{
	ticks_t tn = ticks_t::zero();
	ticks_t tw = ticks_t::zero();
	int cn = 0;
	int cw = 0;
protected:
	GJ::ResQueue* resQueue;
public:
	watcher(GJ::ResQueue* resQueue) : resQueue(resQueue) { }

	void notify()
	{
		++cn;
		ticks_p p = ticks_now();
		notify_x();
		tn += (ticks_now() - p);
	}

	void pop(GJ& gj)
	{
		++cw;
		ticks_p p = ticks_now();
		pop_x(gj);
		tw += (ticks_now() - p);
	}

	virtual ~watcher()
	{
		auto fn = fp_ms(tn).count(), fw = fp_ms(tw).count();
		auto fn_ = fn/cn, fw_ = fw/cw;
		std::cout << "\t\twatcher-> (" << fn << "/" << cn << "=" << fn_ << ") + (" << fw << "/" << cw << "=" << fw_ << ")\n";
	}

protected:
	virtual void notify_x() = 0;
	virtual void pop_x(GJ& gj) = 0;
};

class watcher_sleep : public watcher
{
public:
	watcher_sleep(GJ::ResQueue* resQueue) : watcher(resQueue) { }

	void notify_x() override { }
	void pop_x(GJ& gj) override
	{
		while(!resQueue->pop(gj))
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
};

class watcher_z : public watcher
{
public:
	watcher_z(GJ::ResQueue* resQueue) : watcher(resQueue) { }

	void notify_x() override { }
	void pop_x(GJ& gj) override
	{
		while(!resQueue->pop(gj))
		{
		}
	}
};

class watcher_sl : public watcher
{
	std::atomic_bool flag;
public:
	watcher_sl() = default;
	watcher_sl(const watcher_sl&) = delete;

	watcher_sl(GJ::ResQueue* resQueue) : watcher(resQueue) { }

	void notify_x() override
	{
		flag.store(true,std::memory_order_release);
	}
	void pop_x(GJ& gj) override
	{
		flag.store(false,std::memory_order_release);
		while(!resQueue->pop(gj))
		{
			bool val = false;
			if(flag.compare_exchange_strong(val, val, std::memory_order_acquire))
			{
				while(false == flag.load(std::memory_order_acquire));
			}
		}
	}
};

class watcher_cv : public watcher
{
	std::mutex m;
	std::condition_variable cv;
public:
	watcher_cv(GJ::ResQueue* resQueue) : watcher(resQueue) { }

	void notify_x() override
	{
		std::lock_guard<std::mutex> lk(m); // not clear is it required
		cv.notify_one();
	}
	void pop_x(GJ& gj) override
	{
		if(!resQueue->pop(gj))
		{
			std::unique_lock<std::mutex> lk(m);
			while(!resQueue->pop(gj))
			{
				cv.wait(lk);
			}
		}
	}
};

class watcher_efd : public watcher
{
	int efd;
	int64_t cnt = 0;
public:
	watcher_efd(GJ::ResQueue* resQueue) : watcher(resQueue)
		, efd( eventfd(0, 0) )
	{ }

	void notify_x() override
	{
		int64_t cnt = 1;
		write(efd, &cnt, sizeof(cnt));
	}
	void pop_x(GJ& gj) override
	{
		if(cnt == 0)
		{
			read(efd, &cnt, sizeof(cnt));
		}
		--cnt;
		resQueue->pop(gj);
	}
};

////////
// uncomment required watcher type

//using Watcher = watcher_sleep;
//using Watcher = watcher_z;
//using Watcher = watcher_sl;
using Watcher = watcher_cv;
//using Watcher = watcher_efd;

//////////
/// GraftJob generator
///
GJ_ptr gen_GJ(GJ::ResQueue* rq, Watcher* watcher)
{
//	int len = 2 + std::rand()%10;
	int len = 2 + std::rand()%3;
	std::string s;
	for(int i = 0; i<len; ++i)
	{
		s += ('0' + std::rand()%10);
	}
	std::sort(s.begin(),s.end());
	return GJ_ptr(std::move(s), rq, watcher);
}


using ThreadPoolX = ThreadPoolImpl<FixedFunction<void(), sizeof(GJ_ptr)>,
								  MPMCBoundedQueue>;




///////////////
/// An attempt to implement IO-thread. maybe useful in future for reference. skip the namespace for now.
///

namespace io_th
{
template<typename T>
class opoc_deque
{
	size_t size;
	std::atomic<size_t> cnt;
	std::atomic_bool notify_not_empty;
	std::mutex m_not_empty;
	std::condition_variable cv_not_empty;
	std::atomic_bool notify_not_full;
	std::mutex m_not_full;
	std::condition_variable cv_not_full;
	MPMCBoundedQueue<T> deque;
public:
	opoc_deque(size_t size) : size(size), cnt(0), notify_not_empty(false), notify_not_full(false), deque(size) { }
	~opoc_deque() = default;
	opoc_deque(const opoc_deque&) = delete;
	opoc_deque& operator = (const opoc_deque&) = delete;
	opoc_deque(opoc_deque&&) = default;
	opoc_deque& operator = (opoc_deque&&) = default;
public:
	template<typename U>
	void push(U&& data)
	{
		if(cnt == size)
		{//overflow
			std::unique_lock<std::mutex> lock(m_not_full);
			notify_not_full = true;
			cv_not_full.wait(lock, [this] () { return cnt < size; });
			notify_not_full = false;
		}
		deque.push(std::forward<U>(data));
		++cnt;
		if(cnt == 1 && notify_not_empty)
		{
			std::unique_lock<std::mutex> lock(m_not_empty);
			cv_not_empty.notify_all();
		}
	}

	bool pop(T& data, bool block = false)
	{
		while(true)
		{
			bool res = deque.pop(data);
			if(res) break;
			if(!block) return res;
			std::unique_lock<std::mutex> lock(m_not_empty);
			notify_not_empty = true;
			cv_not_empty.wait(lock, [this] { return 0 < cnt; });
			notify_not_empty = false;
		}
		--cnt;
		if(notify_not_full)
		{
			std::unique_lock<std::mutex> lock(m_not_full);
			cv_not_full.notify_all();
		}
		return true;
	}
};

void IO_thread_test_client()
{
	opoc_deque<std::unique_ptr<std::string>> client_que(32);
	constexpr int n = 200;
	//////client
	std::thread client(
				[&client_que]()
	{
		for(int i=1; i<n; ++i)
		{
			for(int j=0; j<i; ++j)
			{
				std::ostringstream ss;
				ss << i << " " << j << " my string";
				client_que.push(std::unique_ptr<std::string>( new std::string(ss.str())));
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
	);
	//////////
	for(int i=0; i<(n*(n-1))/2; ++i)
	{
		std::unique_ptr<std::string> us;
		client_que.pop(us, true);
//		std::cout << us.get()->c_str() << "\n";
	}
	client.join();
}

void IO_thread_test()
{
	opoc_deque<std::unique_ptr<std::string>> client_que(32);
	constexpr int n = 200;
	//////client
	std::thread client(
				[&client_que]()
	{
		for(int i=1; i<n; ++i)
		{
			for(int j=0; j<i; ++j)
			{
				std::ostringstream ss;
				ss << i << " " << j << " my string";
				client_que.push(std::unique_ptr<std::string>( new std::string(ss.str())));
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
	);
	//////
	opoc_deque<std::unique_ptr<std::string>> crypto_in(64);
	opoc_deque<std::unique_ptr<std::string>> crypto_out(64);
	/////// cryptoNode
	std::thread crypto(
				[&crypto_in, &crypto_out]()
	{
		while(true)
		{
			std::unique_ptr<std::string> us;
			crypto_in.pop(us, true);
			if(*us == "stop") return;
			*us += " crypto";
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			crypto_out.push(std::move(us));
		}
	}
	);
	////////// IO
	for(int i=0; i<(n*(n-1))/2; ++i)
	{
		{//client -> crypto_in
			std::unique_ptr<std::string> us;
			bool res = client_que.pop(us, false);
			if(res)
			{
				crypto_in.push(std::move(us));
				continue;
			}
		}
		std::unique_ptr<std::string> us;
		bool res = crypto_out.pop(us, false);
		if(!res) continue;
		std::cout << us.get()->c_str() << "\n";

	}
	crypto_in.push(std::unique_ptr<std::string>( new std::string("stop")));
	client.join();
	crypto.join();
}

}//namespace io_th


int main(int, const char* [])
{
/*
	io_th::IO_thread_test();
	return 0;

	{
		std::cout << "sizeof(GJ_ptr)->" << sizeof(GJ_ptr) << std::endl;
	}

	test_efd();
*/
    std::cout << "Benchmark job reposting" << std::endl;

	{
		ThreadPoolOptions th_op;
//		th_op.setThreadCount(3);
		th_op.setQueueSize(32);
//		th_op.setQueueSize(4);
		ThreadPoolX thread_pool(th_op);

		size_t resQueueSize;
		{//nearest ceiling power of 2
			size_t val = th_op.threadCount()*th_op.queueSize();
			size_t bit = 1;
			for(; bit<val; bit <<= 1);
			resQueueSize = bit;
		}

		const size_t maxinputSize = th_op.threadCount()*th_op.queueSize();
		assert(maxinputSize == th_op.threadCount()*th_op.queueSize());
		GJ::ResQueue resQueue(resQueueSize);

		//check sizes
		assert(th_op.threadCount()*th_op.queueSize() <= resQueueSize); // Set optimal size

		Watcher watcher(&resQueue);

		ticks_p tm_s = ticks_now();
		ticks_t tm_load = ticks_t::zero();

		for(int i = 0; i<500; ++i)
		{

			int jcnt = maxinputSize;
			for(int i = 0; i<jcnt; ++i)
			{
				thread_pool.post( gen_GJ( &resQueue, &watcher ), true );
			}

			while(jcnt)
			{
				GJ gj;
				watcher.pop(gj);
				--jcnt;
				tm_load += gj.getReturnCode();
			}
		}

		ticks_p tm_e = ticks_now();
		ticks_t tm_all = tm_e - tm_s;

		int cnt = thread_pool.dump_info();
//		std::cout << "\nall->" << cnt << " " << (double)(tm) / (double)1000000 <<  "\n";
		std::cout << "\nall->" << cnt << " " << fp_ms(tm_all - tm_load).count() << " = " << fp_ms(tm_all).count() << " - " <<  fp_ms(tm_load).count() <<  "ms\n";

		return 0;
	}

#ifdef WITH_ASIO
    {
        std::cout << "***asio thread pool***" << std::endl;

        size_t workers_count = std::thread::hardware_concurrency();
        if(0 == workers_count)
        {
            workers_count = 1;
        }

        AsioThreadPool asio_thread_pool(workers_count);

        std::promise<void> waiters[CONCURRENCY];
        for(auto& waiter : waiters)
        {
            asio_thread_pool.post(RepostJob(&asio_thread_pool, &waiter));
        }

        for(auto& waiter : waiters)
        {
            waiter.get_future().wait();
        }
    }
#endif

    return 0;
}
