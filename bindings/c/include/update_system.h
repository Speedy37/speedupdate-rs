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

extern void c_free_string(char *ptr);
extern char* c_update_workspace(
  const char* workspace_path,
  const char* repository_url,
  const char* username,
  const char* password,
  const char* goal_version,
  int (*progress_callback)(const CGlobalProgression*, void*),
  void *data
);

extern char* c_copy_workspace(
  const char* workspace_from,
  const char* workspace_dest
);