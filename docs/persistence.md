# Persistence

mstream can persist job and service definitions to survive restarts. By default, both are stored in-memory and rebuilt from the config file on each restart.

## Job Lifecycle Persistence

mstream keeps job definitions and runtime state in an internal job-lifecycle store so that restarts are predictable. You can opt into a persistent backend by pointing the system section at a managed service:

```toml
[system.job_lifecycle]
service_name = "mongodb-source"
resource = "job-lifecycle"
startup_state = "seed_from_file"
```

### Configuration

| Field | Description |
|-------|-------------|
| `service_name` | References an existing service entry (e.g., MongoDB) that will host the lifecycle collection |
| `resource` | The database collection/table used to store job metadata |
| `startup_state` | Controls how jobs are reconciled when mstream boots (see below) |

### Startup States

| Value | Behavior |
|-------|----------|
| `force_from_file` | Stop everything and start only the connectors listed in the config file |
| `seed_from_file` | Initialize the store from the file if empty, otherwise resume persisted state |
| `keep` | Ignore the file and resume whatever is already stored (for API-driven workflows) |

With a persistent store in place you can:
- Safely restart the server without losing job intent
- Resume jobs automatically
- Keep jobs paused until explicitly restarted via the API

## Service Registry Persistence

Services created through the config file or the `/services` API can survive restarts by enabling a persistent service registry.

```toml
[system]
encryption_key_path = "./mstream-services.key"

[system.service_lifecycle]
service_name = "mongodb-source"
resource = "mstream-services"
```

### Configuration

| Field | Description |
|-------|-------------|
| `service_name` | Must reference an existing MongoDB service; mstream reuses the same client and database |
| `resource` | The collection that will hold the encrypted service documents |
| `encryption_key_path` | Path to the 32-byte AES-256 key file (default: `./mstream.key`) |

### Encryption Key

Service definitions contain sensitive credentials and are encrypted at rest using AES-256.

- If the key file is missing, mstream generates one with strict file permissions
- **Back up this key** — it's required to decrypt persisted services
- Alternative: set `MSTREAM_ENC_KEY` environment variable to the hex-encoded key (useful for container secrets managers)

### Behavior

With `service_lifecycle` configured:
- Service definitions registered via config or API are written to MongoDB
- Services are restored on startup
- Services remain available to jobs even if the process restarts