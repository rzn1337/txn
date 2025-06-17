from feast import Entity, Feature, FeatureView, FileSource, ValueType, BigQuerySource
from datetime import timedelta
# from feast import registry
from feast import (
    Entity,
    FeatureService, 
    FeatureView,
    Field,
    FileSource,
    Project,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String 

transaction = Entity(name="transaction", join_keys=["transaction_id"])

labels_source = FileSource(
    path="s3://finance1337/labels/",
    timestamp_field="label_date",
    event_timestamp_column="label_time"
)

transaction_source = BigQuerySource(
    table="txns-462308.dwh.fact_transactions",
    timestamp_field="transaction_timestamp",
    created_timestamp_column="created_at"
)

is_fraud = Field(name="is_fraud", dtype=Int64)

labels_fv = FeatureView(
    name="fraud_labels",
    entities=[transaction],
    ttl=timedelta(days=90),
    schema=[is_fraud],
    online=True,
    source=labels_source
)

transaction_fv = FeatureView(
    name="transaction_features",
    entities=[transaction],
    ttl=timedelta(days=90),
    schema=[
        Field(name="amount", dtype=Float64),
        Field(name="channel", dtype=String),
    ],
    online=True,
    source=transaction_source
)

# registry.apply([transaction, transaction_fv, labels_fv])