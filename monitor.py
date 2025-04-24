#!/usr/bin/env python3
"""
Monitoring Dashboard Module

This script provides a simple monitoring dashboard for the MapReduce system.
It displays the status of mapper and reducer tasks, as well as system metrics.
"""
import os
import time
import redis
import curses
import argparse
import json
from datetime import datetime


class MapReduceMonitor:
    def __init__(self, redis_host='localhost', redis_port=6379):
        """
        Initialize the monitor.
        
        Args:
            redis_host (str): Redis server hostname
            redis_port (int): Redis server port
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe('mapper_completed', 'reducer_completed')
        
        # Initialize metrics
        self.mapper_events = []
        self.reducer_events = []
        self.start_time = datetime.now()
    
    def update_metrics(self):
        """
        Update metrics from Redis.
        """
        # Process any new messages
        message = self.pubsub.get_message()
        while message:
            if message and message['type'] == 'message':
                channel = message['channel']
                data = json.loads(message['data'])
                timestamp = datetime.now()
                
                if channel == 'mapper_completed':
                    self.mapper_events.append({
                        'timestamp': timestamp,
                        'worker_id': data['worker_id'],
                        'chunk': data['chunk']
                    })
                elif channel == 'reducer_completed':
                    self.reducer_events.append({
                        'timestamp': timestamp,
                        'worker_id': data['worker_id'],
                        'reducer_file': data['reducer_file']
                    })
            
            message = self.pubsub.get_message()
    
    def run_dashboard(self, stdscr):
        """
        Run the monitoring dashboard.
        
        Args:
            stdscr: Curses standard screen
        """
        # Set up curses
        curses.curs_set(0)  # Hide cursor
        curses.start_color()
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
        
        # Main loop
        while True:
            # Update metrics
            self.update_metrics()
            
            # Clear screen
            stdscr.clear()
            
            # Get screen dimensions
            max_y, max_x = stdscr.getmaxyx()
            
            # Display header
            header = "MapReduce Monitoring Dashboard"
            stdscr.addstr(0, (max_x - len(header)) // 2, header, curses.A_BOLD)
            
            # Display runtime
            runtime = datetime.now() - self.start_time
            runtime_str = f"Runtime: {runtime}"
            stdscr.addstr(1, max_x - len(runtime_str) - 1, runtime_str)
            
            # Display mapper progress
            total_mapper_tasks = int(self.redis_client.get('total_mapper_tasks') or 0)
            completed_mapper_tasks = int(self.redis_client.get('completed_mapper_tasks') or 0)
            
            stdscr.addstr(3, 2, "Mapper Progress:", curses.A_BOLD)
            progress_text = f"{completed_mapper_tasks}/{total_mapper_tasks} tasks completed"
            stdscr.addstr(3, 20, progress_text)
            
            if total_mapper_tasks > 0:
                progress_pct = completed_mapper_tasks / total_mapper_tasks
                progress_width = max_x - 30
                filled_width = int(progress_width * progress_pct)
                
                # Draw progress bar
                stdscr.addstr(4, 2, "[")
                stdscr.addstr(4, 3, "=" * filled_width, curses.color_pair(1))
                stdscr.addstr(4, 3 + progress_width, "]")
                stdscr.addstr(4, 5 + progress_width, f"{progress_pct:.1%}")
            
            # Display reducer progress
            total_reducer_tasks = int(self.redis_client.get('total_reducer_tasks') or 0)
            completed_reducer_tasks = int(self.redis_client.get('completed_reducer_tasks') or 0)
            
            stdscr.addstr(6, 2, "Reducer Progress:", curses.A_BOLD)
            progress_text = f"{completed_reducer_tasks}/{total_reducer_tasks} tasks completed"
            stdscr.addstr(6, 20, progress_text)
            
            if total_reducer_tasks > 0:
                progress_pct = completed_reducer_tasks / total_reducer_tasks
                progress_width = max_x - 30
                filled_width = int(progress_width * progress_pct)
                
                # Draw progress bar
                stdscr.addstr(7, 2, "[")
                stdscr.addstr(7, 3, "=" * filled_width, curses.color_pair(1))
                stdscr.addstr(7, 3 + progress_width, "]")
                stdscr.addstr(7, 5 + progress_width, f"{progress_pct:.1%}")
            
            # Display recent mapper events
            stdscr.addstr(9, 2, "Recent Mapper Events:", curses.A_BOLD)
            for i, event in enumerate(reversed(self.mapper_events[-5:])):
                if 10 + i < max_y:
                    time_str = event['timestamp'].strftime("%H:%M:%S")
                    event_str = f"{time_str} - {event['worker_id']} completed {event['chunk']}"
                    stdscr.addstr(10 + i, 4, event_str, curses.color_pair(1))
            
            # Display recent reducer events
            reducer_start_y = min(16, max_y - 10)
            if reducer_start_y < max_y:
                stdscr.addstr(reducer_start_y, 2, "Recent Reducer Events:", curses.A_BOLD)
                for i, event in enumerate(reversed(self.reducer_events[-5:])):
                    if reducer_start_y + 1 + i < max_y:
                        time_str = event['timestamp'].strftime("%H:%M:%S")
                        event_str = f"{time_str} - {event['worker_id']} completed {event['reducer_file']}"
                        stdscr.addstr(reducer_start_y + 1 + i, 4, event_str, curses.color_pair(1))
            
            # Display instructions
            if max_y - 2 > 0:
                stdscr.addstr(max_y - 2, 2, "Press 'q' to quit", curses.A_BOLD)
            
            # Refresh screen
            stdscr.refresh()
            
            # Check for user input (with timeout)
            stdscr.timeout(1000)  # 1 second timeout
            key = stdscr.getch()
            if key == ord('q'):
                break


def main():
    parser = argparse.ArgumentParser(description="MapReduce Monitoring Dashboard")
    parser.add_argument("--redis-host", default="localhost", help="Redis server hostname")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
    
    args = parser.parse_args()
    
    monitor = MapReduceMonitor(
        redis_host=args.redis_host,
        redis_port=args.redis_port
    )
    
    # Run the dashboard with curses
    curses.wrapper(monitor.run_dashboard)


if __name__ == "__main__":
    main()
