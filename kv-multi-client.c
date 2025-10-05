#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdatomic.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define VALUE_MIN 8
#define VALUE_MAX 64
#define DEFAULT_KEY_SPACE 10000
#define CONNECT_RETRY_MS_BASE 50
#define CONNECT_RETRY_MS_LIMIT 1000

static _Atomic int stop_flag = 0;
static _Atomic int next_key = 1;
static _Atomic int max_key_created = 0;

static _Atomic unsigned long long global_success_reqs = 0;
static _Atomic unsigned long long global_failed_reqs = 0;
static _Atomic unsigned long long global_total_latency_ns = 0ULL;

typedef struct {
  char server_ip[64];
  int server_port;
  int num_threads;
  int duration_sec;
  int key_space;
} config_t;

typedef struct {
  int tid;
  unsigned long long local_success;
  unsigned long long local_failed;
  unsigned long long local_reqs;
  unsigned long long local_total_latency_ns;
} thread_stat_t;

config_t cfg;

ssize_t read_n(int fd, void *vptr, size_t n) {
  size_t  nleft = n;
  ssize_t nread;
  char   *ptr = vptr;

  while (nleft > 0) {
    if ((nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR) continue;
      return -1;
    } else if (nread == 0) {
      return (ssize_t)(n - nleft);
    }
      nleft -= nread;
      ptr += nread;
  }
  return (ssize_t)n;
}

ssize_t write_n(int fd, const void *vptr, size_t n) {
    size_t  nleft = n;
    ssize_t nwritten;
    const char *ptr = vptr;

    while (nleft > 0) {
      if ((nwritten = write(fd, ptr, nleft)) <= 0) {
        if (nwritten < 0 && errno == EINTR) continue;
        return -1;
      }
      nleft -= nwritten;
      ptr += nwritten;
    }
    return (ssize_t)n;
}

static inline long long now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int connect_to_server(const char *ip, int port) {
  int s = socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) return -1;
  struct sockaddr_in serv;
  memset(&serv, 0, sizeof(serv));
  serv.sin_family = AF_INET;
  serv.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, ip, &serv.sin_addr) != 1) {
    close(s);
    return -1;
  }
  if (connect(s, (struct sockaddr *)&serv, sizeof(serv)) < 0) {
    perror("connect failed");
    close(s);
    return -1;
  }
  return s;
}

static int send_req(int sockfd, char op, int key, int vlen, const char *value) {
  unsigned char header[9];
  header[0] = (unsigned char)op;
  int32_t key_n = htonl(key);
  int32_t vlen_n = htonl(vlen);
  memcpy(&header[1], &key_n, 4);
  memcpy(&header[5], &vlen_n, 4);

  if (write_n(sockfd, header, 9) != 9) return -1;
  if (vlen > 0 && write_n(sockfd, value, (size_t)vlen) != vlen) return -1;

  unsigned char rheader[5];
  ssize_t rd = read_n(sockfd, rheader, 5);
  if (rd != 5) return -1;

  char status = (char)rheader[0];
  int32_t payload_size_n;
  memcpy(&payload_size_n, &rheader[1], 4);
  int payload_size = ntohl(payload_size_n);
  if (payload_size < 0) return -1;

  char *payload = NULL;
  if (payload_size > 0) {
    payload = malloc(payload_size + 1);
    if (!payload) {
      char tmp[1024];
      int to_read = payload_size;
      while (to_read > 0) {
        int chunk = to_read > (int)sizeof(tmp) ? (int)sizeof(tmp) : to_read;
        ssize_t x = read_n(sockfd, tmp, chunk);
        if (x <= 0) break;
        to_read -= x;
      }
      return status == 'S' ? 1 : 0;
    }
    ssize_t got = read_n(sockfd, payload, payload_size);
    if (got != payload_size) {
      free(payload);
      return -1;
    }
    free(payload);
  }
  return status == 'S' ? 1 : 0;
}

// op_type: '0'->'Create', '1'->'Read', '2'->'Update, '3'->'Delete'
static void * worker_thread(void *arg) {
  thread_stat_t st = {0};
  st.tid = (int)(long)arg;

  unsigned int randstate = (unsigned int)time(NULL) ^ (unsigned int)(st.tid * 0x9e3779b9);
  int sock = -1;

  int retry_ms = CONNECT_RETRY_MS_BASE;
  while (!stop_flag && sock < 0) {
    sock = connect_to_server(cfg.server_ip, cfg.server_port);
    if (sock < 0) {
      usleep(retry_ms * 1000);
      retry_ms *= 2;
      if (retry_ms > CONNECT_RETRY_MS_LIMIT) retry_ms = CONNECT_RETRY_MS_LIMIT;
    }
  }
  if (sock < 0) return NULL;

  while(!stop_flag) {
    int op_type = 0; 
    int key = 0;
    int vlen = 0;
    char valbuf[VALUE_MAX];

    int cur_next = atomic_load(&next_key);
    int cur_max_created = atomic_load(&max_key_created);

    if(cur_next <= cfg.key_space) {
      int r = rand_r(&randstate) % 100;
      if (r < 50) op_type = 0;
      else {
        if(cur_max_created <= 0) op_type = 0;
        else {
          int r2 = rand_r(&randstate) % 100;
          if(r2 < 70) op_type = 1;
          else if(r2 < 90) op_type = 2;
          else op_type = 3;
        }
      }
    } else {
      int r = rand_r(&randstate) % 100;
      if (r < 70) op_type = 1;
      else if (r < 90) op_type = 2;
      else op_type = 3;
    }

    if(op_type == 0) {
      int allocated = atomic_fetch_add(&next_key, 1);
      if(allocated <= cfg.key_space) {
        key = allocated;
        vlen = VALUE_MIN + (rand_r(&randstate) % (VALUE_MAX - VALUE_MIN + 1));
        for (int i = 0; i < vlen; ++i) valbuf[i] = 'A' + (char)( (rand_r(&randstate) % 26) );
        int prev = atomic_load(&max_key_created);
        while (prev < key && !atomic_compare_exchange_weak(&max_key_created, &prev, key)){}
      } else {
        if (atomic_load(&max_key_created) > 0) {
          key = 1 + (rand_r(&randstate) % atomic_load(&max_key_created));
          op_type = 1;
        } else {
          usleep(1000);
          continue;
        }
      } 
    } else {
      int maxk = atomic_load(&max_key_created);
      if(maxk <= 0) continue;
      key = 1 + (rand_r(&randstate) % maxk);
      if (op_type == 2 || op_type == 0) {
        vlen = VALUE_MIN + (rand_r(&randstate) % (VALUE_MAX - VALUE_MIN + 1));
        for (int i = 0; i < vlen; ++i) valbuf[i] = 'a' + (char)( (rand_r(&randstate) % 26) );
      }
    }

    long long t0 = now_ns();
    int res = send_req(sock, 
      op_type==0 ? 'C' : op_type==1 ? 'R': op_type==2 ? 'U' : 'D',
      key,
      vlen,
      vlen>0 ? valbuf : NULL);
    
    long long t1 = now_ns();
    long long latency = (t1 - t0);

    st.local_reqs++;
    if (res == 0) {
        st.local_success++;
        st.local_total_latency_ns += (unsigned long long)latency;
    } else if (res == 1) {
        st.local_failed++;
    } else {
        st.local_failed++;
        close(sock);
        sock = -1;
        int backoff = CONNECT_RETRY_MS_BASE;
        while (!stop_flag) {
            sock = connect_to_server(cfg.server_ip, cfg.server_port);
            if (sock >= 0) break;
            usleep((unsigned int)backoff * 1000);
            backoff *= 2;
            if (backoff > CONNECT_RETRY_MS_LIMIT) backoff = CONNECT_RETRY_MS_LIMIT;
        }
        if (sock < 0) break;
    }
  }
  __atomic_fetch_add(&global_success_reqs, st.local_success, __ATOMIC_RELAXED);
  __atomic_fetch_add(&global_failed_reqs, st.local_failed, __ATOMIC_RELAXED);
  __atomic_fetch_add(&global_total_latency_ns, st.local_total_latency_ns, __ATOMIC_RELAXED);
  if (sock >= 0) close(sock);
  return NULL;
}

int main(int argc, char *argv[]) {
  if(!(argc==5 || argc==6)) {
    fprintf(stderr, "Usage: %s <server-ip> <server-port> <num-threads> <duration-sec> [key-space]\n", argv[0]);
    exit(EXIT_FAILURE);
  }

  strncpy(cfg.server_ip, argv[1], sizeof(cfg.server_ip)-1);
  cfg.server_ip[sizeof(cfg.server_ip)-1] = '\0';
  cfg.server_port = atoi(argv[2]);
  cfg.num_threads = atoi(argv[3]);
  cfg.duration_sec = atoi(argv[4]);
  cfg.key_space = argc == 6 ? atoi(argv[5]): DEFAULT_KEY_SPACE;

  if (cfg.num_threads<=0 || cfg.duration_sec<=0 || cfg.server_port<=0 || cfg.key_space<1000) {
    fprintf(stderr, "Invalid arguments.\n");
    exit(EXIT_FAILURE);
  }

  atomic_store(&next_key, 1);
  atomic_store(&max_key_created, 0);
  atomic_store(&stop_flag, 0);
  atomic_store(&global_success_reqs, 0);
  atomic_store(&global_failed_reqs, 0);
  atomic_store(&global_total_latency_ns, 0ULL);

  pthread_t *threads = calloc((size_t)cfg.num_threads, sizeof(pthread_t));
  if (!threads) { perror("calloc"); exit(EXIT_FAILURE); }

  for(int i=0; i<cfg.num_threads; i++) {
    if(pthread_create(&threads[i], NULL, worker_thread, (void *)(long) i) != 0) {
      perror("pthread_create");
      threads[i] = 0;
    }
  }

  printf("Running load: server=%s:%d threads=%d duration=%d sec key-space=%d\n",
        cfg.server_ip, cfg.server_port, cfg.num_threads, cfg.duration_sec, cfg.key_space);
  fflush(stdout);

  struct timespec start_ts, end_ts;
  clock_gettime(CLOCK_MONOTONIC, &start_ts);
  sleep((unsigned)cfg.duration_sec);
  atomic_store(&stop_flag, 1);
  clock_gettime(CLOCK_MONOTONIC, &end_ts);

  for (int i = 0; i < cfg.num_threads; ++i) {
    if (threads[i]) pthread_join(threads[i], NULL);
  }

  long long elapsed_ns = (long long)(end_ts.tv_sec - start_ts.tv_sec) * 1000000000LL + (end_ts.tv_nsec - start_ts.tv_nsec);
  double elapsed_s = (double)elapsed_ns / 1e9;

  unsigned long long succ = atomic_load(&global_success_reqs);
  unsigned long long fail = atomic_load(&global_failed_reqs);
  unsigned long long total_lat_ns = atomic_load(&global_total_latency_ns);

  double throughput = (double)succ / elapsed_s;
  double avg_resp_ms = (succ > 0) ? ((double)total_lat_ns / (double)succ) / 1e6 : 0.0;

  printf("\n---- Results ----\n");
  printf("Duration (s): %.3f\n", elapsed_s);
  printf("Successful requests: %llu\n", (unsigned long long)succ);
  printf("Failed requests: %llu\n", (unsigned long long)fail);
  printf("Throughput (req/s): %.2f\n", throughput);
  printf("Average response time (ms): %.4f\n", avg_resp_ms);
  printf("Unique keys created (approx): %d\n", atomic_load(&max_key_created));
  printf("------------------\n");

  free(threads);
  return 0;
}