# kschedule
Kafka-based "task" scheduler

## Overview
Scheduling is a common requirement in modern software stacks. This project implements a generic scheduler using Apache Kafka as the backing store. Messages are delivered to the scheduler via one or more input topics, and delivered to a desired output topic at the scheduled time. 


## Operating Mode

* KTable Mode: In memory buffer for upcoming tasks.
* KStreams Only Mode: No in memory buffer, increased network usage, lower fidelity scheduling.