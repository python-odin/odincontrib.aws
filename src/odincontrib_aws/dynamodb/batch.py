import logging
from collections import defaultdict

from odin.compatibility import deprecated
from odin.utils import chunk, getmeta

logger = logging.getLogger("odincontrib.aws.dynamodb.batch")

MAX_DYNAMO_BATCH_SIZE = 25


@deprecated("Please use odincontrib_aws.dyanmodb.session.batch_write_items")
def batch_write(client, resources, batch_size=MAX_DYNAMO_BATCH_SIZE, batch_counter_step=MAX_DYNAMO_BATCH_SIZE):
    """
    Batch write table resources to DynamoDB

    :param client: DynamoDB client
    :param resources: Iterable of resources to batch load.
    :param batch_size: Size of each batch.
    :param batch_counter_step: Number of batches loaded between each counter message.

    """
    idx = 0
    item_count = 0

    batch = defaultdict(list)
    for idx, batch_resources in enumerate(chunk(resources, batch_size)):
        batch.clear()
        for resource in batch_resources:
            batch[getmeta(resource).table_name(client)].append(
                {'PutRequest': {'Item': resource.to_dynamo_dict(skip_null_fields=True)}}
            )
            item_count += 1

        if (idx % batch_counter_step) == 0:
            logger.info("Loading batch: %s", idx)

        client.batch_write_item(RequestItems=batch)

    logger.info("Loaded %s records in %s batches.", item_count, idx + 1)
