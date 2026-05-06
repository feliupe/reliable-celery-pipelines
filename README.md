# reliable-celery-pipelines

Reference implementation of fault-tolerant async task orchestration with Celery.
Each failure mode is demonstrated in a broken baseline and then fixed in an
isolated, runnable script.

Start here: [`0_naive.py`](./0_naive.py) — broken baseline demonstrating FM-1
(pipeline dies mid-way on partial failure; the chord callback never fires).
