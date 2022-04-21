import concurrent.futures
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

path_to_cred = "./credentials/firestore_serviceAccount_Performance_testing.json"
cred = credentials.Certificate(path_to_cred)

firebase_admin.initialize_app(cred)

db = firestore.client()

lock_ref = db.collection("test_transaction").document("lock")

POOL_WORKERS = 50
ATTEMPTS = 1000
TRANSACTION_MAX_RETRY = 5
LOCK_ID_VOLUME = TRANSACTION_MAX_RETRY * 20
print(
    "ATTEMPTS: %d, TRANSACTION_MAX_RETRY: %d, LOCK_ID_VOLUME: %d"
    % (ATTEMPTS, TRANSACTION_MAX_RETRY, LOCK_ID_VOLUME)
)


def is_lock_existed_or_set(firestore_client, lock_ref, request_id: int, lock_id: str):
    print("%d is started." % request_id)
    transaction = firestore_client.transaction(max_attempts=TRANSACTION_MAX_RETRY)
    snapshot = lock_ref.collection("package_lock").document(lock_id)

    @firestore.transactional
    def get_lock_or_set_in_transaction(transaction, lock_ref, lock_id):
        lock_data = lock_ref.get(transaction=transaction).to_dict()
        if lock_data is not None and lock_id in lock_data:
            return True
        transaction.set(lock_ref, {lock_id: firestore.SERVER_TIMESTAMP}, merge=True)
        return False

    try:
        result = get_lock_or_set_in_transaction(transaction, snapshot, lock_id)
    except ValueError as e:
        return request_id, lock_id, None, ("ValueError: %s" % e)
    return request_id, lock_id, result, None


def custom_callback(future):
    id, lock_id, result, exception = future.result()
    if result is not None:
        print("%d is done: (lock_id = %s, result = %s)" % (id, lock_id, result))
    else:
        print("%d is failed: (lock_id = %s, exception = %s)" % (id, lock_id, exception))


with concurrent.futures.ThreadPoolExecutor(max_workers=POOL_WORKERS) as executor:
    futures = {
        executor.submit(
            is_lock_existed_or_set, db, lock_ref, id, str(id // (LOCK_ID_VOLUME))
        )
        for id in range(ATTEMPTS)
    }
    print("executor: submit all")
    for future in futures:
        future.add_done_callback(custom_callback)
    print("futures: add all callback")
    concurrent.futures.wait(futures)
