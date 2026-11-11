/* C interface of libglass. It is the base for bindings to other languages.
 *
 * Objects are opaque pointers, created by a glass_*_new or connect function and
 * released by the matching glass_*_free. A function that creates an object gives
 * NULL if it fails, and one that gives an int gives one of the GLASS_* statuses.
 * glass_last_error() describes the last failure of the calling thread.
 *
 * Data given by the library must be released with glass_free_bytes().
 * Node id 1 means everybody. In a remote variable, id 0 means the local value.
 */

#ifndef LIBGLASS_H
#define LIBGLASS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GLASS_OK 0
/* Nothing arrived in time, or there is nothing to give. */
#define GLASS_EMPTY 1
#define GLASS_ERROR (-1)

#define GLASS_EVERYBODY 1u

typedef struct GlassServer GlassServer;
typedef struct GlassClient GlassClient;
typedef struct GlassEvent GlassEvent;
typedef struct GlassShared GlassShared;
typedef struct GlassBarrier GlassBarrier;
typedef struct GlassMessenger GlassMessenger;
typedef struct GlassRemote GlassRemote;
typedef struct GlassAlias GlassAlias;

const char *glass_last_error(void);
void glass_free_bytes(uint8_t *data, size_t len);

/* How long connecting may take, for every connection opened from now on (10 s). */
void glass_set_connect_timeout(uint64_t timeout_ms);

/* Addresses are "host:port". Port 0 lets the system choose. */
GlassServer *glass_server_new(const char *addr);
/* seed is any server of the group, or NULL to start a new one. */
GlassServer *glass_server_new_peer(const char *addr, const char *seed);
/* The same through SCTP, for clients that use glass_client_connect_sctp. They give
 * NULL where the system does not have SCTP, as on Windows. */
GlassServer *glass_server_new_sctp(const char *addr);
GlassServer *glass_server_new_peer_sctp(const char *addr, const char *seed);
uint16_t glass_server_port(const GlassServer *server);
size_t glass_server_total_nodes(const GlassServer *server);
size_t glass_server_peer_count(const GlassServer *server);
int glass_server_wait(const GlassServer *server);
void glass_server_free(GlassServer *server);

GlassClient *glass_client_connect(const char *addr, const char *nickname);
GlassClient *glass_client_connect_sctp(const char *addr, const char *nickname);
/* Connect again when the server is lost, trying for up to window_ms each time; 0, the
 * default, turns it off. See GlassClient::set_reconnect for what is kept. */
void glass_client_set_reconnect(const GlassClient *client, uint64_t window_ms);
uint32_t glass_client_id(const GlassClient *client);
int glass_client_is_master(const GlassClient *client);
int glass_client_is_connected(const GlassClient *client);
size_t glass_client_total_nodes(const GlassClient *client);
int glass_client_kill_all(const GlassClient *client);
void glass_client_free(GlassClient *client);

/* In all functions below, global is 0 for Local mode, anything else for Global. */

GlassEvent *glass_event_new(const GlassClient *client, const char *name, int global);
int glass_event_enqueue(const GlassEvent *event, const uint8_t *data, size_t len,
                        uint32_t target, int send_to_self);
int glass_event_get(const GlassEvent *event, uint64_t timeout_ms, uint8_t **out,
                    size_t *out_len);
/* The same, and the id of the client that sent the event. */
int glass_event_get_with_sender(const GlassEvent *event, uint64_t timeout_ms, uint32_t *from,
                                uint8_t **out, size_t *out_len);
size_t glass_event_queue_size(const GlassEvent *event);
void glass_event_free(GlassEvent *event);

GlassShared *glass_shared_new(const GlassClient *client, const char *name,
                              const uint8_t *data, size_t len, int global);
int glass_shared_set(const GlassShared *shared, const uint8_t *data, size_t len);
int glass_shared_get(const GlassShared *shared, uint8_t **out, size_t *out_len);
int glass_shared_send_update(const GlassShared *shared);
int glass_shared_get_update(const GlassShared *shared);
void glass_shared_free(GlassShared *shared);

GlassBarrier *glass_barrier_new(const GlassClient *client, uint32_t number,
                                uint32_t group, int active, int global);
int glass_barrier_sync(const GlassBarrier *barrier);
/* Gives up after timeout_ms, still counting as arrived: the next call waits for the
 * same release. Returns GLASS_EMPTY if it gave up. */
int glass_barrier_sync_timeout(const GlassBarrier *barrier, uint64_t timeout_ms);
void glass_barrier_free(GlassBarrier *barrier);

GlassMessenger *glass_messenger_new(const GlassClient *client, const char *name,
                                    int global);
int glass_messenger_send(const GlassMessenger *messenger, uint32_t target,
                         const uint8_t *data, size_t len);
int glass_messenger_get(const GlassMessenger *messenger, uint64_t timeout_ms,
                        uint32_t *from, uint8_t **out, size_t *out_len);
void glass_messenger_free(GlassMessenger *messenger);

GlassRemote *glass_remote_new(const GlassClient *client, const char *name,
                              const uint8_t *data, size_t len);
int glass_remote_set(const GlassRemote *remote, uint32_t id, const uint8_t *data,
                     size_t len);
int glass_remote_get(const GlassRemote *remote, uint32_t id, uint64_t timeout_ms,
                     uint8_t **out, size_t *out_len);
void glass_remote_free(GlassRemote *remote);

GlassAlias *glass_alias_new(const GlassClient *client, const char *name, int global);
int glass_alias_associate(const GlassAlias *alias, uint32_t id, const char *association);
int glass_alias_association(const GlassAlias *alias, uint8_t **out, size_t *out_len);
void glass_alias_free(GlassAlias *alias);

#ifdef __cplusplus
}
#endif

#endif /* LIBGLASS_H */
