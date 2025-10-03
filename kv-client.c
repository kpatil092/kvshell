#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <stdint.h>

#define INITIAL_BUF_SIZE 128

static int sockfd = -1;

char *read_line(FILE *fp) {
  size_t size = INITIAL_BUF_SIZE;
  size_t len = 0;
  char *buf = malloc(size);
  if (!buf) return NULL;

  int ch;
  while ((ch = fgetc(fp)) != EOF) {
    if (ch == '\n') break;

    if (len + 1 >= size) {
      size *= 2;
      char *new_buf = realloc(buf, size);
      if (!new_buf) {
        free(buf);
        return NULL;
      }
      buf = new_buf;
    }

    buf[len++] = (char)ch;
  }

  if (len == 0 && ch == EOF) {
    free(buf);
    return NULL;
  }

  buf[len] = '\0';
  return buf;
}

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

static int send_request(char op, int key, const char *value, int vlen) {
  if (sockfd < 0) {
    printf("ERROR: no active connection\n");
    return -1;
  }

  unsigned char header[9];
  header[0] = (unsigned char)op;
  int32_t key_n = htonl(key);
  int32_t vlen_n = htonl(vlen);
  memcpy(&header[1], &key_n, 4);
  memcpy(&header[5], &vlen_n, 4);

  if (write_n(sockfd, header, 9) != 9) {
      perror("write header");
      return -1;
  }
  if (vlen > 0 && write_n(sockfd, value, (size_t)vlen) != vlen) {
    perror("write value");
    return -1;
  }

  unsigned char rheader[5];
  ssize_t rd = read_n(sockfd, rheader, 5);
  if (rd == 0) {
    printf("ERROR: server closed connection\n");
    close(sockfd);
    sockfd = -1;
    return -1;
  } else if (rd != 5) {
    printf("ERROR: failed to read response header\n");
    return -1;
  }

  char status = (char)rheader[0];
  int32_t payload_size_n;
  memcpy(&payload_size_n, &rheader[1], 4);
  int payload_size = ntohl(payload_size_n);
  if (payload_size < 0) {
    printf("ERROR: server sent negative payload size\n");
    return -1;
  }

  char *payload = NULL;
  if (payload_size > 0) {
    payload = malloc(payload_size + 1);
    if (!payload) {
      printf("ERROR: client out of memory\n");
      char tmp[1024];
      int to_read = payload_size;
      while (to_read > 0) {
        int chunk = to_read > (int)sizeof(tmp) ? (int)sizeof(tmp) : to_read;
        ssize_t x = read_n(sockfd, tmp, chunk);
        if (x <= 0) break;
        to_read -= x;
      }
      return -1;
    }
    ssize_t got = read_n(sockfd, payload, payload_size);
    if (got != payload_size) {
      free(payload);
      printf("ERROR: failed to read full payload\n");
      return -1;
    }
    payload[payload_size] = '\0';
  }

  if (status == 'O') { // OK
    if (op == 'R') {
      if (payload_size > 0) {
        fwrite(payload, 1, payload_size, stdout);
        putchar('\n');
      } else {
        printf("OK\n");
      }
    } else {
      if (payload_size > 0) {
        printf("%.*s\n", payload_size, payload);
      } else {
        printf("OK\n");
      }
    }
  } else if (status == 'E') {
    if (payload_size > 0) {
      printf("ERROR: %.*s\n", payload_size, payload);
    } else {
      printf("ERROR: server returned an error\n");
    }
  } else {
      printf("ERROR: unknown status from server\n");
  }

  if (payload) free(payload);
  return 0;
}

static int do_connect(const char *ip, int port) {
  if (sockfd >= 0) {
    printf("ERROR: already connected\n");
    return -1;
  }
  int s = socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) { perror("socket"); return -1; }

  struct sockaddr_in serv;
  memset(&serv, 0, sizeof(serv));
  serv.sin_family = AF_INET;
  serv.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, ip, &serv.sin_addr) != 1) {
    close(s);
    printf("ERROR: invalid IP address\n");
    return -1;
  }

  if (connect(s, (struct sockaddr *)&serv, sizeof(serv)) < 0) {
    perror("connect");
    close(s);
    return -1;
  }

  sockfd = s;
  printf("OK\n");
  return 0;
}

static int do_disconnect(void) {
  if (sockfd < 0) {
    printf("ERROR: no active connection\n");
    return -1;
  }
  close(sockfd);
  sockfd = -1;
  printf("OK\n");
  return 0;
}

static void handle_command_line(char *line) {
  char *p = line;
  while (*p && (*p == ' ' || *p == '\t')) p++;
  if (*p == '\0') return;

  char cmd[32];
  int n = 0;
  int offset = 0;
  if (sscanf(p, "%31s%n", cmd, &n) != 1) return;
  p += n;
  if (strcmp(cmd, "connect") == 0) {
    char ipbuf[64];
    int port;
    if (sscanf(p, "%63s%n", ipbuf, &n) != 1) {
      printf("ERROR: connect requires <server-ip> <server-port>\n");
      return;
    }
    p += n;
    if (sscanf(p, "%d%n", &port, &n) != 1) {
      printf("ERROR: connect requires <server-ip> <server-port>\n");
      return;
    }
    do_connect(ipbuf, port);
  } else if (strcmp(cmd, "disconnect") == 0) {
    do_disconnect();
  } else if (strcmp(cmd, "create") == 0 || strcmp(cmd, "update") == 0) {
    int key, vlen; int n1=0;
    if (sscanf(p, "%d%n", &key, &n1) != 1) {
      printf("ERROR: %s requires <key> <value-size> <value>\n", cmd);
      return;
    }
    p += n1;
    if (sscanf(p, "%d%n", &vlen, &n1) != 1) {
      printf("ERROR: %s requires <key> <value-size> <value>\n", cmd);
      return;
    }
    p += n1;
    while (*p == ' ' || *p == '\t') p++;
    if (vlen < 0) {
      printf("ERROR: negative value-size\n");
      return;
    }

    size_t actual_len = strlen(p);
    if (actual_len > (size_t)vlen) actual_len = vlen;

    char *value_buf = malloc(actual_len);
    if (!value_buf) {
      printf("ERROR: out of memory\n");
      return;
    }

    memcpy(value_buf, p, actual_len);
    vlen = (int)actual_len;

    if (strcmp(cmd, "create") == 0) {
      send_request('C', key, value_buf, vlen);
    } else {
      send_request('U', key, value_buf, vlen);
    }
    free(value_buf);

  } else if (strcmp(cmd, "read") == 0) {
    int key, n1;
    if (sscanf(p, "%d%n", &key, &n1) != 1) {
      printf("ERROR: read requires <key>\n");
      return;
    }
    send_request('R', key, NULL, 0);
  } else if (strcmp(cmd, "delete") == 0) {
    int key, n1;
    if (sscanf(p, "%d%n", &key, &n1) != 1) {
      printf("ERROR: delete requires <key>\n");
      return;
    }
    send_request('D', key, NULL, 0);
  } else {
    printf("ERROR: unknown command\n");
  }
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s interactive\n       %s batch <file>\n", argv[0], argv[0]);
    exit(EXIT_FAILURE);
  }

  signal(SIGPIPE, SIG_IGN);

  int is_interactive = 0;
  FILE *fp = NULL;

  if (strcmp(argv[1], "interactive") == 0) {
    is_interactive = 1;
  } else if (strcmp(argv[1], "batch") == 0) {
    if (argc < 3) {
      fprintf(stderr, "Batch mode requires a filename\n");
      exit(EXIT_FAILURE);
    }
    fp = fopen(argv[2], "r");
    if (!fp) {
      perror("fopen");
      exit(EXIT_FAILURE);
    }
  } else {
    fprintf(stderr, "First argument must be 'interactive' or 'batch'\n");
    exit(EXIT_FAILURE);
  }

  if (is_interactive) {
    while (1) {
      printf("kv> ");
      fflush(stdout);
      char *line = read_line(stdin);
      if (!line) break;
      handle_command_line(line);
      free(line);
    }
  } else {
      int batch_fd = fileno(fp);
      int saved_stdin = dup(STDIN_FILENO);
      if (dup2(batch_fd, STDIN_FILENO) == -1) {
        perror("dup2");
        fclose(fp);
        exit(EXIT_FAILURE);
      }

      char *line;
      while ((line = read_line(fp)) != NULL) {
        handle_command_line(line);
        free(line);
      }

      dup2(saved_stdin, STDIN_FILENO);
      close(saved_stdin);
      fclose(fp);
  }

  if (sockfd >= 0) close(sockfd);
  return 0;
}
