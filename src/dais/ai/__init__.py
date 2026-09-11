"""AI-native extensions to DAIS - Phase 8.

Everything under this package is additive: it reads specs and database
state that already exist, but never changes how an existing pipeline
runs by default, and is never invoked automatically as part of a
pipeline execution. See dq_recommender.py (Phase 8a) and
profiler.py / anomaly_detector.py / anomaly_explainer.py (Phase 8b).
"""
