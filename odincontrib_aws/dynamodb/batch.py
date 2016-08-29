import logging
from collections import defaultdict
from odin.utils import chunk

logger = logging.getLogger("odincontrib.aws.dynamodb.batch")

MAX_DYNAMO_BATCH_SIZE = 25


def batch_write(client, resources, batch_size=MAX_DYNAMO_BATCH_SIZE, batch_debug_count=MAX_DYNAMO_BATCH_SIZE):
    """
    Batch write table resources to DynamoDB

    :param client: DynamoDB client
    :param resources: Iterable of resources to batch load.
    :param batch_size: Size of each batch.
    :param batch_debug_count: Number of batches loaded between each DEBUG message.

    """
    idx = 0
    batch = defaultdict(list)
    for idx, batch_resources in enumerate(chunk(resources, batch_size)):
        batch.clear()
        for resource in batch_resources:
            batch[resource.__class__.format_table_name(client)].append(
                {'PutRequest': {'Item': resource.to_dynamo_dict(skip_null_fields=True)}}
            )

        if logger.isEnabledFor(logging.DEBUG) and (idx % batch_debug_count) == 0:
            logger.debug("Loading batch: %s", idx)

        client.batch_write_item(RequestItems=batch)

    logger.info("Loaded %s records in %s batches.", (idx * batch_size) + len(batch), idx + 1)
