#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <stdint.h>

#define MAX_LEN 256
#define BUFFER_SIZE 256

typedef struct kv_node {
  int key;
  int vlen;
  char *value;
} kv_node_t;

int kv_idx = 0;

static kv_node_t *kv_table[MAX_LEN];

ssize_t read_n(int fd, void *vptr, size_t n) {
  size_t nleft = n;
  ssize_t nread;
  char *ptr = vptr;

  while (nleft > 0) {
    if ((nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR) continue;
      return -1;
    } else if (nread == 0) {
      return (ssize_t)(n - nleft); /* EOF */
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

static int send_response(int confd, char status, const void *payload, int payload_size) {
  unsigned char header[5];
  header[0] = (unsigned char)status;
  int32_t ps = htonl(payload_size);
  memcpy(&header[1], &ps, 4);

  if (write_n(confd, header, 5) != 5) return -1;
  if (payload_size > 0 && write_n(confd, payload, payload_size) != payload_size) 
    return -1;
  return 0;
}

static int kv_find(int key) {
  for(int i=0; i<kv_idx; i++) {
    if(kv_table[i]->key == key) return i;
  }
  return -1;
}

static int kv_create(int key, int vlen, const char *value) {
  if (kv_find(key) != -1) return -1; 
  if(kv_idx == MAX_LEN) return -3;
  kv_node_t *node = malloc(sizeof(kv_node_t));
  if (!node) return -2;
  node->key = key;
  node->value = malloc(vlen);
  if (!node->value) { free(node); return -2; }
  memcpy(node->value, value, vlen);
  node->vlen = vlen;

  kv_table[kv_idx++] = node;
  return 0;
}

static int kv_update(int key, int vlen, const char *value) {
  int idx = kv_find(key);
  if (idx == -1) return -1;
  kv_node_t *node = kv_table[idx];
  char *newv = malloc(vlen);
  if (!newv) return -2;
  free(node->value);
  node->value = newv;
  memcpy(node->value, value, vlen);
  node->vlen = vlen;
  return 0;
}

static int kv_delete(int key) {
  int idx = kv_find(key);
  if(idx == -1) return -1;
  kv_node_t *node = kv_table[idx];
  free(node->value);
  free(node);

  for(int i=idx; i<kv_idx-1; i++) {
    kv_table[i] = kv_table[i+1];
  }
  kv_table[--kv_idx] = NULL;
  return 0;
}

int main(int argc, char *argv[]) {
  if (argc != 3)   {
    fprintf(stderr, "Usage: %s <bind-ip> <port>\n", argv[0]);
    exit(0);
  }

  signal(SIGPIPE, SIG_IGN);

  const char *bind_ip = argv[1];
  int port = atoi(argv[2]);
  if (port <= 0)   {
    fprintf(stderr, "Invalid port\n");
    exit(EXIT_FAILURE);
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)   {
    perror("socket");
    exit(EXIT_FAILURE);
  }

  int yes = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)   {
    perror("setsockopt");
    close(fd);
    exit(EXIT_FAILURE);
  }

  struct sockaddr_in servaddr;
  memset(&servaddr, 0, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = htons((uint16_t)port);
  if(inet_pton(AF_INET, bind_ip, &servaddr.sin_addr) != 1) {
    fprintf(stderr, "Invalid bind IP: %s\n", bind_ip);
    close(fd);
    exit(EXIT_FAILURE);
  }

  if(bind(fd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
    perror("bind");
    close(fd);
    exit(EXIT_FAILURE);
  }

  if(listen(fd, 5) < 0) {
    perror("listen");
    close(fd);
    exit(EXIT_FAILURE);
  }

  printf("KV server listening on %s:%d\n", bind_ip, port);

  memset(kv_table, 0, sizeof(kv_table));

  while(1) {
    struct sockaddr_in cliaddr;
    socklen_t clien = sizeof(cliaddr);
    int confd = accept(fd, (struct sockaddr *)&cliaddr, &clien);
    if (confd < 0) {
      if (errno == EINTR) continue;
      perror("accept");
      continue;
    }

    char clie_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &cliaddr.sin_addr, clie_ip, sizeof(clie_ip));
    printf("Client connected from %s:%d\n", clie_ip, ntohs(cliaddr.sin_port));

    while(1) {
      unsigned char header[9];
      ssize_t r = read_n(confd, header, 9);
      if (r == 0) { /* client closed */
        printf("Client disconnected\n");
        close(confd);
        break;
      } else if (r < 0) {
        perror("read header");
        close(confd);
        break;
      } else if (r != 9) {
        fprintf(stderr, "Incomplete header read (%zd bytes). Closing.\n", r);
        close(confd);
        break;
      } 
      
      char op = (char)header[0];
      int32_t key_n, vlen_n;
      memcpy(&key_n, &header[1], 4);
      memcpy(&vlen_n, &header[5], 4);
      int key = ntohl(key_n);
      int vlen = ntohl(vlen_n);
      if(vlen < 0) {
        const char *err = "Invalid value size";
        send_response(confd, 'E', err, (int)strlen(err));
        // readn(confd, malloc(1), (size_t)(-vlen));
        continue;
      }

      char *value = NULL;
      if((op == 'C' || op == 'U') && vlen > 0) {
        value = malloc(vlen);
        if (!value) {
          const char *err = "Server out of memory";
          send_response(confd, 'E', err, (int)strlen(err));
          char discard_buf[1];
          read_n(confd, discard_buf, (size_t)vlen);
          continue;
        }

        ssize_t n = read_n(confd, value, (size_t)vlen);
        if (n != vlen) {
            free(value);
            const char *err = "Failed to read value bytes (client disconnect?)";
            send_response(confd, 'E', err, (int)strlen(err));
            close(confd);
            break;
        }
      }

      if(op == 'C') {
        int res_c = kv_create(key, vlen, value);
        if(value) free(value);
        if(res_c == 0) {
          const char *ok = "OK";
          send_response(confd, 'O', ok, 2);
        } else if (res_c == -1) {
          const char *err = "Key already exists";
          send_response(confd, 'E', err, (int)strlen(err));
        } else if(res_c == -2) {
          const char *err = "Create failed (server error)";
          send_response(confd, 'E', err, (int)strlen(err));
        } else {
          const char *err = "Create failed (table size reached)";
          send_response(confd, 'E', err, (int)strlen(err));
        }
      } else if(op == 'R') {
        int idx = kv_find(key);
        if (idx == -1) {
            const char *err = "Key not found";
            send_response(confd, 'E', err, (int)strlen(err));
        } else {
          kv_node_t *node = kv_table[idx];
          send_response(confd, 'O', node->value, node->vlen);
        }
        
      } else if(op == 'U') {
        int res_u = kv_update(key, vlen, value);
        if (value) free(value);
        if (res_u == 0) {
          const char *ok = "OK";
          send_response(confd, 'O', ok, 2);
        } else if (res_u == -1) {
          const char *err = "Key does not exist";
          send_response(confd, 'E', err, (int)strlen(err));
        } else {
          const char *err = "Update failed (server error)";
          send_response(confd, 'E', err, (int)strlen(err));
        }
      } else if(op == 'D') {
        int res_d = kv_delete(key);
        if (res_d == 0) {
          const char *ok = "OK";
          send_response(confd, 'O', ok, 2);
        } else {
          const char *err = "Key does not exist";
          send_response(confd, 'E', err, (int)strlen(err));
        }
      } else {
        const char *err = "Unknown operation";
        send_response(confd, 'E', err, (int)strlen(err));
        if (value) free(value);
      }
    }
  }

  close(fd);
  return 0;
}
