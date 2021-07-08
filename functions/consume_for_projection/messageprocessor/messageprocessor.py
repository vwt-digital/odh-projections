import logging
import sys

from config import FIELDS_TO_SKIP

# from gobits import Gobits
# from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)


class MessageProcessor(object):
    def __init__(self):
        print("joe")
        self.original_object = None

    def process(self, payload):
        # Get schema
        # TODO: replace with schema API
        schema = "schema.json"
        # Get properties of schema
        self.original_object = schema.get("properties")
        if not self.original_object:
            sys.exit(0)
        # Make lists of fields in schema
        current_field_list, schema_field_list = self.list_of_schema_fields(
            self.original_object, [], []
        )
        # Check if last value does not have sub fields and if it has, remove them
        schema_field_list_copy = []
        for sf in schema_field_list:
            sf_copy = []
            for field in sf:
                sf_copy.append(field)
            schema_field_list_copy.append(sf_copy)
        schema_field_list = self.remove_if_subfields(
            schema_field_list, schema_field_list_copy
        )

    #     message = payload["message"]
    #     if self.process_message(message) is False:
    #         logging.info("Message not processed")
    #     else:
    #         logging.info("Message is processed")

    # def process_message(self, message):
    #     return True

    def list_of_schema_fields(self, json_object, current_field_list, current_list):
        if isinstance(json_object, dict):
            type_object = json_object.get("type")
            # If type_object is a dictionary, it is a subfield, not an information field
            if isinstance(type_object, dict):
                # Create the path to the current field and add it to current list of all paths to field
                current_field_list, json_object, current_list = self.add_path(
                    current_field_list, json_object, current_list
                )
            # Check if type of object is array, if it is get its items to get its subfields
            elif type_object == "array":
                json_object = json_object["items"]
                current_field_list, current_list = self.list_of_schema_fields(
                    json_object, current_field_list, current_list
                )
            # Check if type of object is object, if it is get its properties to get its subfields
            elif type_object == "object":
                json_object = json_object["properties"]
                current_field_list, current_list = self.list_of_schema_fields(
                    json_object, current_field_list, current_list
                )
            # If object does not have a type field, it is an object with subfields
            elif not type_object:
                # Create the path to the current field and add it to current list of all paths to field
                current_field_list, json_object, current_list = self.add_path(
                    current_field_list, json_object, current_list
                )
        return current_field_list, current_list

    def add_path(self, current_field_list, json_object, current_list):
        # Hard copy the current field list
        current_field_list_copy = []
        for cf in current_field_list:
            current_field_list_copy.append(cf)
        # For every subfield
        for field in json_object:
            if field in FIELDS_TO_SKIP:
                continue
            # Add it to the fields that came before it
            current_field_list.append(field)
            # Recursively run function on object in field to check if it has subfields
            current_field_list, current_list = self.list_of_schema_fields(
                json_object[field], current_field_list, current_list
            )
            # Add the new list with fields that came before this field (including this field) to the list with all the lists
            current_list.append(current_field_list)
            # Clear list because the end of the path was found
            current_field_list = []
            # But do add the fields that came before without the current field because the next field is still in this branch
            current_field_list.extend(current_field_list_copy)
        return current_field_list, json_object, current_list

    def remove_if_subfields(self, original_json_fields, json_fields):
        # Create list with fields to remove from list because they are not "end fields" in path (e.g. they contain subfields)
        to_remove = []
        # For every list of fields (e.g. a path to a field)
        for i in range(len(json_fields)):
            # Get last value of path in original json object
            last_value = self.get_last_value(json_fields[i], self.original_object)
            # Check if last value is a json object
            if isinstance(last_value, dict):
                # If it is, make sure it does not have any subfields
                last_value_type = last_value.get("type")
                if last_value_type == "object" or last_value_type == "array":
                    to_remove.append(i)
        for to_rem_item in sorted(to_remove, reverse=True):
            del original_json_fields[to_rem_item]
        return original_json_fields

    def get_last_value(self, field_list, last_object):
        # Get first field in path of fields
        new_field = field_list.pop(0)
        type_object = last_object.get("type")
        # If field type is array or object, the new field will return nothing
        # if used to get the new object from the last object
        # The current last object has to be gotten from the items or properties field
        if type_object == "array":
            # The current last object in the path is in the "items" field
            last_object = last_object["items"]
            # Check if this object is of type "object"
            type_items_object = last_object.get("type")
            if type_items_object == "object":
                # If it is, make this the last object, otherwise keep the items field as last object
                last_object = last_object["properties"]
        elif type_object == "object":
            # The current last object can be found in the properties field
            last_object = last_object["properties"]
        # Else the current last object is correct
        else:
            last_value = last_object
        # Get the new last current object by getting it via the first value in the path of fields list
        new_object = last_object.get(new_field)
        # If the path of fields is now empty, the last value (object) of the field path is found
        if not field_list:
            last_value = new_object
        # If the field returns a new last object, run the function recursively on this new last object
        elif new_object:
            last_value = self.get_last_value(field_list, new_object)
        # If the new last current object cannot be found via the path, the path is wrong
        elif not new_object:
            logging.error(
                f"Object {last_object} does not contain field {new_field}, field path is wrong"
            )
            sys.exit(1)
        return last_value
