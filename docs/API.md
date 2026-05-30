# Forgejo Artifact Cache API Specification

This document details the API endpoints, request/response formats, and required headers for the Forgejo Artifact Cache server as implemented in `act/artifactcache`.

## Base URL
All endpoints are prefixed with: `/_apis/artifactcache`

## Authentication & Metadata Headers
These headers are mandatory for every request to authenticate and identify the context of the cache operation.

| Header | Description | Example | Required |
| :--- | :--- | :--- | :--- |
| `Forgejo-Cache-Repo` | The full name of the repository (e.g., `owner/repo`). | `forgejo/runner` | Yes |
| `Forgejo-Cache-RunNumber` | The Actions run number. | `42` | Yes |
| `Forgejo-Cache-Timestamp` | Current Unix timestamp (seconds). | `1717084800` | Yes |
| `Forgejo-Cache-MAC` | HMAC-SHA256 signature. | `5e884898da28...` | Yes |
| `Forgejo-Cache-WriteIsolationKey` | Key used for cache isolation (e.g., branch name). | `refs/heads/main` | Optional |
| `Forgejo-Cache-Host` | Base URL of the cache server (used in `find` response). | `http://10.0.0.1:8080` | Yes (for Find) |
| `Forgejo-Cache-RunId` | The unique ID of the current run. | `123456789` | Yes (for Find) |

### HMAC Calculation
The `Forgejo-Cache-MAC` is an HMAC-SHA256 signature of the following string using a shared secret:
`{repo}>{runNumber}>{timestamp}>{writeIsolationKey}`

**Example:**
*   `repo`: `forgejo/runner`
*   `runNumber`: `42`
*   `timestamp`: `1717084800`
*   `writeIsolationKey`: `refs/heads/main`
*   **String to sign:** `forgejo/runner>42>1717084800>refs/heads/main`

---

## Endpoints

### 1. Find Cache
Locates a cache entry matching the provided keys and version.

*   **Method:** `GET`
*   **Path:** `/_apis/artifactcache/cache`
*   **Query Parameters:**
    *   `keys` (string): Comma-separated list of cache keys (case-insensitive).
        *   *Example:* `Linux-node-20,Linux-node-`
    *   `version` (string): The cache version (usually a hash of the environment/dependencies).
        *   *Example:* `b10a8db164e0754105b7a99be72e3fe5`
*   **Response:**
    *   **200 OK**: Cache hit.
        **Template:**
        ```json
        {
          "result": "hit",
          "archiveLocation": "{host}/{runId}/_apis/artifactcache/artifacts/{id}",
          "cacheKey": "matched-key"
        }
        ```
        **Example:**
        ```json
        {
          "result": "hit",
          "archiveLocation": "http://10.0.0.1:8080/123456789/_apis/artifactcache/artifacts/55",
          "cacheKey": "linux-node-20"
        }
        ```
    *   **204 No Content**: Cache miss.
    *   **403 Forbidden**: HMAC validation failed (`{"error": "validation error"}`).
    *   **500 Internal Server Error**: Database or storage lookup error (`{"error": "..."}`).

### 2. Reserve Cache
Prepares a new cache entry for uploading.

*   **Method:** `POST`
*   **Path:** `/_apis/artifactcache/caches`
*   **Request Body:**
    **Template:**
    ```json
    {
      "key": "{cacheKey}",
      "version": "{version}",
      "cacheSize": {sizeInBytes}
    }
    ```
    **Example:**
    ```json
    {
      "key": "linux-node-20",
      "version": "b10a8db164e0754105b7a99be72e3fe5",
      "cacheSize": 10485760
    }
    ```
    *Note: `cacheSize` is optional or can be `-1` for unknown sizes (backward compatibility).*
*   **Response:**
    *   **200 OK**: Successfully reserved.
        **Template:**
        ```json
        {
          "cacheId": {id}
        }
        ```
    *   **400 Bad Request**: Invalid JSON body (`{"error": "..."}`).
    *   **403 Forbidden**: HMAC validation failed (`{"error": "validation error"}`).
    *   **500 Internal Server Error**: Database error during reservation (`{"error": "..."}`).

### 3. Upload Cache Chunk
Uploads a binary chunk of the cache archive.

*   **Method:** `PATCH`
*   **Path:** `/_apis/artifactcache/caches/:id`
*   **Headers:**
    *   `Content-Range`: `bytes <start>-<stop>/*`
        *   *Example:* `bytes 0-1023/*`
    *   `Content-Type`: `application/octet-stream`.
*   **Request Body:** Binary data of the chunk.
*   **Response:**
    *   **200 OK**: Chunk accepted.
    *   **400 Bad Request**: 
        *   Invalid ID format.
        *   Invalid `Content-Range` header.
        *   Cache is already marked as complete.
    *   **403 Forbidden**: 
        *   HMAC validation failed.
        *   `WriteIsolationKey` mismatch (attempting to upload to a cache reserved with a different isolation key).
    *   **404 Not Found**: Cache ID was not reserved.
    *   **500 Internal Server Error**: Storage write error.

### 4. Commit Cache
Finalizes the cache upload, merging all chunks.

*   **Method:** `POST`
*   **Path:** `/_apis/artifactcache/caches/:id`
*   **Request Body:** Empty.
*   **Response:**
    *   **200 OK**: Cache successfully committed and available for future hits.
    *   **400 Bad Request**: 
        *   Invalid ID format.
        *   Cache is already marked as complete.
    *   **403 Forbidden**: 
        *   HMAC validation failed.
        *   `WriteIsolationKey` mismatch.
    *   **404 Not Found**: Cache ID was not reserved.
    *   **500 Internal Server Error**: Error during finalization or database update.

### 5. Get Cache (Download)
Downloads the full cache archive.

*   **Method:** `GET`
*   **Path:** `/_apis/artifactcache/artifacts/:id`
*   **Response:**
    *   **200 OK**: Binary stream of the cache archive.
    *   **400 Bad Request**: Invalid ID format.
    *   **403 Forbidden**: 
        *   HMAC validation failed.
        *   `WriteIsolationKey` mismatch (access denied if cache is isolated and key does not match).
    *   **404 Not Found**: Cache ID not found.
    *   **500 Internal Server Error**: Storage or database retrieval error.

### 6. Clean Cache
Placeholder for cache cleanup (currently returns success without action).

*   **Method:** `POST`
*   **Path:** `/_apis/artifactcache/clean`
*   **Response:**
    *   **200 OK**: `{}`
    *   **403 Forbidden**: HMAC validation failed.

---

## Variable Reference & Examples

| Component | Variable | Example Value | Description |
| :--- | :--- | :--- | :--- |
| **Headers** | `Forgejo-Cache-Repo` | `forgejo/runner` | Repository full name. |
| | `Forgejo-Cache-RunNumber` | `42` | The `GITHUB_RUN_NUMBER`. |
| | `Forgejo-Cache-Timestamp` | `1717084800` | Unix timestamp of the request. |
| | `Forgejo-Cache-WriteIsolationKey` | `refs/heads/main` | Often the branch or tag name. |
| | `Forgejo-Cache-Host` | `http://10.0.0.1:8080` | URL where the runner can reach the cache server. |
| | `Forgejo-Cache-RunId` | `123456789` | The `GITHUB_RUN_ID`. |
| **Query Params** | `keys` | `Linux-node-20,Linux-node-` | Primary and fallback search keys. |
| | `version` | `b10a8db164...` | Hash of dependency files. |
| **Request Body** | `key` | `linux-node-20` | The exact key used to save the cache. |
| | `cacheSize` | `10485760` | Size in bytes. `-1` means unknown. |
| **Path Params** | `:id` | `55` | The numeric ID returned by the Reserve endpoint. |

---

## Implementation Details
*   **Case Insensitivity:** Cache keys are converted to lowercase before storage and lookup.
*   **Isolation:** If `WriteIsolationKey` is provided, `find` first looks for a match with that key. If not found, it falls back to a search with an empty isolation key.
*   **Storage:** Chunks are stored temporarily indexed by their offset. Upon `commit`, the server concatenates these chunks in order and verifies the total size matches the `cacheSize` provided during reservation (if specified).
