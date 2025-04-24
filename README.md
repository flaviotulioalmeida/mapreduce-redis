# MapReduce with Redis - Distributed Systems Implementation

This project implements a distributed MapReduce system using Redis for task coordination and message passing. The system simulates a cluster of mappers and reducers that process data collaboratively in multiple stages: Map, Shuffle, and Reduce.

## Architecture

The system consists of the following components:

1. **Coordinator**: Orchestrates the entire MapReduce process, including splitting the input file, pushing tasks to Redis queues, and merging the final results.

2. **Mapper Workers**: Process chunks of the input file and emit key-value pairs.

3. **Shuffler**: Groups values by key and partitions them for reducers.

4. **Reducer Workers**: Process grouped key-value pairs and produce the final output.

5. **Redis**: Used for task coordination, message passing, and worker communication.

### System Flow

1. The coordinator splits the input file into chunks.
2. Mapper tasks are pushed to a Redis queue.
3. Mapper workers fetch tasks from the queue and process them.
4. After all mappers finish, the shuffler groups values by key.
5. Reducer tasks are pushed to a Redis queue.
6. Reducer workers fetch tasks from the queue and process them.
7. The coordinator merges all reducer outputs into a final result.

## Requirements

- Python 3.6+
- Redis server
- Python packages:
  - redis

## Installation

1. Clone the repository:

\`\`\`bash
git clone https://github.com/flaviotulioalmeida/mapreduce-redis.git
cd mapreduce-redis
\`\`\`

2. Install the required Python packages:

\`\`\`bash
pip install -r requirements.txt
\`\`\`

3. Install and start Redis server:


\`\`\`bash
# On Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis-server

# On macOS with Homebrew
brew install redis
brew services start redis

# On Windows, download and install Redis from https://github.com/tporadowski/redis/releases
\`\`\`

## Usage

### 1. Generate Test Data


First, generate a large text file for testing:

\`\`\`bash
python data_generator.py --size-mb 1000 --output-file data.txt
\`\`\`

This will create a 1GB text file with random words.

### 2. Run the MapReduce Job

You can run the entire MapReduce job using the coordinator:

\`\`\`bash
python coordinator.py --input-file data.txt --num-chunks 10 --num-reducers 5
\`\`\`

### 3. Run Workers Separately

Alternatively, you can run the coordinator and workers separately:

1. Start the coordinator:

\`\`\`bash
python coordinator.py --input-file data.txt
\`\`\`

2. Start multiple mapper workers:

\`\`\`bash
python run_workers.py --num-mappers 3 --num-reducers 2
\`\`\`

### 4. Monitor the Progress

You can monitor the progress of the MapReduce job using the monitoring dashboard:

\`\`\`bash
python monitor.py
\`\`\`

### 5. Using Docker

You can also run the entire system using Docker Compose:

\`\`\`bash
docker-compose up --build
\`\`\`

This will start Redis, the coordinator, and multiple mapper and reducer workers.

## Project Structure

- `coordinator.py`: Orchestrates the entire MapReduce process
- `file_splitter.py`: Splits the input file into chunks
- `mapper.py`: Implements mapper workers
- `shuffler.py`: Implements the shuffle phase
- `reducer.py`: Implements reducer workers
- `run_workers.py`: Starts multiple mapper and reducer workers
- `monitor.py`: Provides a monitoring dashboard
- `data_generator.py`: Generates test data
- `docker-compose.yml`: Docker Compose configuration
- `Dockerfile`: Docker configuration
- `requirements.txt`: Python dependencies

## Customization

### Changing the Map and Reduce Functions

You can customize the map and reduce functions in `mapper.py` and `reducer.py` respectively:

- `map_function` in `mapper.py`: Currently implements word count by emitting (word, 1) for each word.
- `reduce_function` in `reducer.py`: Currently implements sum by adding all values for each key.

### Scaling the System

You can scale the system by adjusting the number of chunks and reducers:

\`\`\`bash
python coordinator.py --num-chunks 20 --num-reducers 10
\`\`\`

Or by running more worker processes:

\`\`\`bash
python run_workers.py --num-mappers 5 --num-reducers 3
\`\`\`

## Bonus Features

This implementation includes several bonus features:

1. **Redis Pub/Sub**: Used for notifying when tasks are complete.
2. **Retry Logic**: Failed tasks are pushed back to the queue for retry.
3. **Progress Monitoring**: A simple monitoring dashboard shows the progress of mapper and reducer tasks.
4. **Docker Support**: Docker and Docker Compose are used to simulate multiple workers.

## Troubleshooting

### Redis Connection Issues

If you encounter Redis connection issues, make sure the Redis server is running:

\`\`\`bash
redis-cli ping
\`\`\`

Should return `PONG`.

### File Permissions

If you encounter file permission issues, make sure the directories have the correct permissions:

\`\`\`bash
mkdir -p chunks intermediate reducer_input output
chmod 777 chunks intermediate reducer_input output
\`\`\`

## License

This project is licensed under the MIT License - see the LICENSE file for details.
