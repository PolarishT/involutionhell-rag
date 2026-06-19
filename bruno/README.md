# Bruno Web API Tests

This collection covers every controller-defined RAG web route, including both
JSON and multipart variants of document creation. It also includes read-only
smoke tests for the exposed Actuator health, info, metrics and loggers
endpoints.

## Run in Bruno

1. Start the API and make sure its base URL is reachable. The bundled `local`
   environment uses `http://127.0.0.1:8080/api/v1`.
2. Open the `bruno` directory as a collection.
3. Select the `local` environment, or edit `baseUrl` for another deployment.
4. Run the collection in sequence.

The collection stores the created `documentId` as a runtime variable and uses a
fixed test user/conversation so the document, indexing, SSE and conversation
requests form one end-to-end scenario.
