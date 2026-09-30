#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "kaddr.h"
#include "kconnection.h"
#include "kselector_manager.h"
#include "kselectable.h"
#include "ksocket.h"
#include "ksync.h"
#include "kfiber.h"
#include "kfiber_sync.h"

extern bool get_addr(const char *hostname, kgl_addr_type addr_type, struct addrinfo **res);

static int64_t now_msec(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static void test_condition_events(void)
{
	kcond *manual = kcond_init(false);
	kcond_notice(manual);
	assert(kcond_try_wait(manual, 0));
	assert(kcond_try_wait(manual, 0));
	kcond_reset(manual);
	assert(!kcond_try_wait(manual, 0));
	kcond_destroy(manual);

	kcond *automatic = kcond_init(true);
	kcond_notice(automatic);
	assert(kcond_try_wait(automatic, 0));
	assert(!kcond_try_wait(automatic, 0));
	int64_t start = now_msec();
	assert(!kcond_try_wait(automatic, 50));
	assert(now_msec() - start >= 40);
	kcond_destroy(automatic);
}

static void test_address_helpers(void)
{
	struct addrinfo *res = NULL;
	assert(!get_addr("invalid host name", kgl_addr_ip, &res));
	assert(res == NULL);

	assert(get_addr("127.0.0.1", kgl_addr_ip, &res));
	struct sockaddr_in *source = (struct sockaddr_in *)res->ai_addr;
	uint16_t old_port = source->sin_port;
	sockaddr_i converted;
	ksocket_addrinfo_sockaddr(res, 12345, &converted);
	assert(source->sin_port == old_port);
	assert(ksocket_addr_port(&converted) == 12345);
	freeaddrinfo(res);

#ifdef KSOCKET_UNIX
	char long_path[sizeof(((struct sockaddr_un *)0)->sun_path) + 1];
	memset(long_path, 'x', sizeof(long_path) - 1);
	long_path[sizeof(long_path) - 1] = '\0';
	struct sockaddr_un unix_addr;
	assert(ksocket_unix_addr(long_path, &unix_addr) == -1);
#endif
}

static void test_socket_helpers(void)
{
	SOCKET fd = socket(AF_INET, SOCK_DGRAM, 0);
	assert(ksocket_opened(fd));
	assert(ksocket_is_block(fd));
	ksocket_no_block(fd);
	assert(!ksocket_is_block(fd));
	ksocket_close(fd);
}

static void test_udp_without_pktinfo(void)
{
	SOCKET receiver = socket(AF_INET, SOCK_DGRAM, 0);
	SOCKET sender = socket(AF_INET, SOCK_DGRAM, 0);
	assert(ksocket_opened(receiver) && ksocket_opened(sender));

	struct sockaddr_in bind_addr;
	memset(&bind_addr, 0, sizeof(bind_addr));
	bind_addr.sin_family = AF_INET;
	bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	assert(bind(receiver, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) == 0);
	socklen_t bind_len = sizeof(bind_addr);
	assert(getsockname(receiver, (struct sockaddr *)&bind_addr, &bind_len) == 0);
	assert(sendto(sender, "x", 1, 0, (struct sockaddr *)&bind_addr, bind_len) == 1);

	char byte = 0;
	kgl_iovec iov;
	iov.iov_base = &byte;
	iov.iov_len = 1;
	kgl_iovec buffers;
	buffers.iov_base = (char *)&iov;
	buffers.iov_len = 1;
	kconnection connection;
	memset(&connection, 0, sizeof(connection));
	connection.st.fd = receiver;
	connection.st.e[OP_READ].buffer = &buffers;
	assert(selectable_recvmsg(&connection.st) == 1);
	assert(byte == 'x');

	ksocket_close(sender);
	ksocket_close(receiver);
}

static kfiber_rwlock *test_rwlock;
static kcond *test_rwlock_done;
static volatile int test_rwlock_reader_done;

static int rwlock_reader(void *arg, int len)
{
	kfiber_rwlock_rlock(test_rwlock);
	kfiber_rwlock_runlock(test_rwlock);
	test_rwlock_reader_done = 1;
	return 0;
}

static int rwlock_writer(void *arg, int len)
{
	kfiber *reader = NULL;
	kfiber_rwlock_wlock(test_rwlock);
	kfiber_create(rwlock_reader, NULL, 0, 0, &reader);
	kfiber_msleep(10);
	/* the reader waits on the same selector, wunlock must not wake it with the internal mutex held. */
	kfiber_rwlock_wunlock(test_rwlock);
	kfiber_join(reader, NULL);
	kcond_notice(test_rwlock_done);
	return 0;
}

static void test_rwlock_wakeup_same_selector_reader(void)
{
	test_rwlock = kfiber_rwlock_init();
	test_rwlock_done = kcond_init(true);
	kfiber_create2(get_selector_by_index(0), rwlock_writer, NULL, 0, 0, NULL);
	if (!kcond_try_wait(test_rwlock_done, 5000) || !test_rwlock_reader_done) {
		fprintf(stderr, "kfiber_rwlock_wunlock deadlocked with a same-selector reader\n");
		exit(1);
	}
	kcond_destroy(test_rwlock_done);
	kfiber_rwlock_destroy(test_rwlock);
}

static kfiber_mutex *test_mutex;
static kcond *test_mutex_done;
static int test_mutex_order[4];
static volatile int test_mutex_got;

static int mutex_waiter(void *arg, int len)
{
	kfiber_mutex_lock(test_mutex);
	test_mutex_order[test_mutex_got++] = len;
	kfiber_mutex_unlock(test_mutex);
	return 0;
}

static int mutex_holder(void *arg, int len)
{
	int fifo_ok, limit_ok;
	kfiber_mutex_lock(test_mutex);
	for (int i = 1; i <= 3; i++) {
		kfiber_create(mutex_waiter, NULL, i, 0, NULL);
	}
	kfiber_msleep(10);
	kfiber_mutex_unlock(test_mutex);
	kfiber_msleep(10);
	fifo_ok = (test_mutex_got == 3 && test_mutex_order[0] == 1 && test_mutex_order[1] == 2 && test_mutex_order[2] == 3);

	test_mutex_got = 0;
	kfiber_mutex_lock(test_mutex);
	kfiber_create(mutex_waiter, NULL, 1, 0, NULL);
	kfiber_create(mutex_waiter, NULL, 2, 0, NULL);
	kfiber_msleep(10);
	kfiber_mutex_set_limit(test_mutex, 3);
	kfiber_msleep(10);
	limit_ok = (test_mutex_got == 2);
	kfiber_mutex_unlock(test_mutex);
	if (fifo_ok && limit_ok) {
		kcond_notice(test_mutex_done);
	} else {
		fprintf(stderr, "kfiber_mutex fifo_ok=%d limit_ok=%d order=%d,%d,%d\n", fifo_ok, limit_ok,
			test_mutex_order[0], test_mutex_order[1], test_mutex_order[2]);
	}
	return 0;
}

static void test_mutex_fifo_and_limit(void)
{
	test_mutex = kfiber_mutex_init();
	test_mutex_done = kcond_init(true);
	kfiber_create2(get_selector_by_index(0), mutex_holder, NULL, 0, 0, NULL);
	if (!kcond_try_wait(test_mutex_done, 5000)) {
		fprintf(stderr, "kfiber_mutex waiters are not FIFO or set_limit does not wake waiters\n");
		exit(1);
	}
	kcond_destroy(test_mutex_done);
	kfiber_mutex_destroy(test_mutex);
}

static void test_selector_shutdown(void)
{
	kasync_init();
	selector_manager_init(1, false);
	selector_manager_start(NULL, true);
#ifdef _WIN32
	Sleep(100);
#else
	usleep(100000);
#endif
	test_rwlock_wakeup_same_selector_reader();
	test_mutex_fifo_and_limit();
	selector_manager_close();
	assert(!is_selector_manager_init());
}

int main(void)
{
	test_condition_events();
	test_address_helpers();
	test_socket_helpers();
	test_udp_without_pktinfo();
	test_selector_shutdown();
	puts("kasync regression tests passed");
	return 0;
}
