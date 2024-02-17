
CREATE TABLE IF NOT EXISTS Requests (
    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_id INTEGER NOT NULL,
    payload TEXT NOT NULL, -- JSON
    status TEXT DEFAULT 'Pending',
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
)


CREATE TABLE IF NOT EXISTS Responses (
    response_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL,
    response_data TEXT,
    status_code INTEGER,
    error_message TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(request_id) REFERENCES Requests(request_id)
)

