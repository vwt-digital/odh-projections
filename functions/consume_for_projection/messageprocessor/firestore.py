import logging

logging.basicConfig(level=logging.INFO)


def upload_to_firestore(db, collection, firestore_entity, primary_key):
    # Upload to firestore
    try:
        # Check if document has a primary key
        if primary_key:
            # If it has, upload the document with an ID based on its value
            primary_key_value = firestore_entity[primary_key]
            doc_ref = db.collection(collection).document(primary_key_value)
        else:
            # Otherwise upload the document with a random ID
            doc_ref = db.collection(collection).document()
        doc_ref.set(firestore_entity)
    except Exception as e:
        if primary_key in firestore_entity:
            logging.exception(
                f"Unable to upload firestore entity {firestore_entity[primary_key]} "
                f"because of {e}"
            )
        else:
            logging.exception(f"Unable to upload firestore entity " f"because of {e}")
        return False
    return True
