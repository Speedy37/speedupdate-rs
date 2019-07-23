#include <stdint.h>

typedef struct {
  const char *version;
  uint8_t update_in_progress;
} CLocalState;

/// Get current version informations
/// if `version` is nullptr, use latest version
extern char* c_local_state(
  const char* workspace_path,
  void (*version_callback)(const char *err, const CLocalState* state, void*),
  void *data
);

typedef struct {
  const char *version;
  const char *description;
} CRemoteVersion;

/// Get version informations
/// if `version` is nullptr, use latest version
extern uint8_t c_version_info(
  const char* repository_url,
  const char* username, /* nullable */
  const char* password, /* nullable */
  const char* version, /* nullable */
  void (*version_callback)(const char *err, const CRemoteVersion* info, void*),
  void *data
);

typedef struct {
  size_t packages_start;
  size_t packages_end;

  size_t downloaded_files_start;
  size_t downloaded_files_end;
  uint64_t downloaded_bytes_start;
  uint64_t downloaded_bytes_end;

  size_t applied_files_start;
  size_t applied_files_end;
  uint64_t applied_input_bytes_start;
  uint64_t applied_input_bytes_end;
  uint64_t applied_output_bytes_start;
  uint64_t applied_output_bytes_end;

  size_t failed_files;

  double downloaded_files_per_sec;
  double downloaded_bytes_per_sec;

  double applied_files_per_sec;
  double applied_input_bytes_per_sec;
  double applied_output_bytes_per_sec;
} CGlobalProgression;

/// Update workspace to goal_version
/// if `goal_version` is nullptr, update to latest version
extern uint8_t c_update_workspace(
  const char* workspace_path,
  const char* repository_url,
  const char* username, /* nullable */
  const char* password, /* nullable */
  const char* goal_version, /* nullable */
  uint8_t (*progress_callback)(const char *err, const CGlobalProgression* progression, void*),
  void *data
);

typedef struct {
  size_t files_start;
  size_t files_end;
  uint64_t bytes_start;
  uint64_t bytes_end;

  size_t failed_files;
} CCopyProgression;

/// Copy workspace from one directory to another one
extern uint8_t c_copy_workspace(
  const char* workspace_from,
  const char* workspace_dest,
  uint8_t (*progress_callback)(const char *err, const void *CCopyProgression, void*),
  void *data
);
