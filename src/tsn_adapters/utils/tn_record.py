from typing import Any, Dict, Hashable, List
import trufnetwork_sdk_py.client as tn_client

def create_record_batches(stream_id: str, records_dict: list[Dict[Hashable, Any]], num_batches: int, records_per_batch: int):
    batches: List[tn_client.RecordBatch] = []
    for batch_idx in range(num_batches):
        start_idx = batch_idx * records_per_batch
        end_idx = start_idx + records_per_batch
        batch_data = records_dict[start_idx:end_idx]
        
        batch_records = []
        for record in batch_data:
            batch_records.append(
                tn_client.Record(
                    date=record["date"],
                    value=float(record["value"])
                )
            )
        
        batches.append(
            tn_client.RecordBatch(
                stream_id=stream_id,
                inputs=batch_records
            )
        )

    return batches