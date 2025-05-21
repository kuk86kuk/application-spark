#!/bin/bash
echo "Starting Hadoop NodeManager..."
export HADOOP_HOME=/opt/hadoop-3.2.1
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export TERM=xterm
export HADOOP_LOG_DIR=$HADOOP_HOME/logs

# Wait for ResourceManager
until nc -z resourcemanager 8032; do
    echo "Waiting for resourcemanager:8032..."
    sleep 2
done

# Create and set permissions for log directory
mkdir -p $HADOOP_LOG_DIR
chmod -R 777 $HADOOP_LOG_DIR

# Start NodeManager
echo "Starting YARN NodeManager..."
$HADOOP_HOME/bin/yarn --daemon start nodemanager
YARN_EXIT_CODE=$?
if [ $YARN_EXIT_CODE -ne 0 ]; then
    echo "Failed to start NodeManager (exit code $YARN_EXIT_CODE). Checking logs..."
    ls -l $HADOOP_LOG_DIR || echo "No log directory found."
    cat $HADOOP_LOG_DIR/hadoop-*-nodemanager-*.log || echo "No hadoop-nodemanager logs found."
    cat $HADOOP_LOG_DIR/yarn-*-nodemanager-*.log || echo "No yarn-nodemanager logs found."
    cat $HADOOP_LOG_DIR/*.out || echo "No .out logs found."
fi

# Wait for log files
echo "Waiting for NodeManager logs..."
for i in {1..30}; do
    if ls $HADOOP_LOG_DIR/hadoop-*-nodemanager-*.log >/dev/null 2>&1 || ls $HADOOP_LOG_DIR/yarn-*-nodemanager-*.log >/dev/null 2>&1; then
        echo "NodeManager logs found."
        break
    fi
    echo "No logs yet, waiting... ($i/30)"
    sleep 2
done

# Check if NodeManager is running
if ! ps aux | grep -v grep | grep -q "org.apache.hadoop.yarn.server.nodemanager.NodeManager"; then
    echo "NodeManager not running. Checking logs..."
    ls -l $HADOOP_LOG_DIR || echo "No log directory found."
    cat $HADOOP_LOG_DIR/hadoop-*-nodemanager-*.log || echo "No hadoop-nodemanager logs found."
    cat $HADOOP_LOG_DIR/yarn-*-nodemanager-*.log || echo "No yarn-nodemanager logs found."
    cat $HADOOP_LOG_DIR/*.out || echo "No .out logs found."
else
    echo "NodeManager is running."
fi

# Keep container running
echo "Keeping container running. Press Ctrl+C to exit or check logs in $HADOOP_LOG_DIR."
tail -f $HADOOP_LOG_DIR/hadoop-*-nodemanager-*.log $HADOOP_LOG_DIR/yarn-*-nodemanager-*.log $HADOOP_LOG_DIR/*.out 2>/dev/null