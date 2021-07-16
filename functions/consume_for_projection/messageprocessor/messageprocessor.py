import json
import logging
import os
import sys

from config import FIELDS_TO_SKIP

logging.basicConfig(level=logging.INFO)


class MessageProcessor(object):
    def __init__(self):
        self.topic_name = os.environ.get("TOPIC_NAME")
        if not self.topic_name:
            logging.error("Environment variable 'TOPIC_NAME' should be set")
            sys.exit(1)
        self.schema_file_path = os.environ.get("SCHEMA_FILE_PATH")
        if not self.schema_file_path:
            logging.error("Environment variable 'SCHEMA_FILE_PATH' should be set")
            sys.exit(1)
        self.original_object = None

    def process(self, payload):
        # Get schema
        with open(self.schema_file_path, "r") as f:
            schema = json.load(f)
        # Get properties of schema
        self.original_object = schema.get("properties")
        if not self.original_object:
            sys.exit(0)
        # Make lists of fields in schema
        current_field_list, schema_field_lists = self.list_of_schema_fields(
            self.original_object, [], []
        )
        # Check if last value does not have sub fields and if it has, remove them
        schema_field_lists_copy = self.copy_list_of_lists(schema_field_lists)
        schema_field_lists = self.remove_if_subfields(
            schema_field_lists, schema_field_lists_copy
        )
        # collection_name = f"projection_{self.topic_name}"
        # Remove values in message that are not conform schema
        self.clean_message(schema_field_lists, payload, 0)
        # Check if values are missing from message that should be there
        for schema_field_list in schema_field_lists:
            check = self.check_for_missing_values_message(schema_field_list, payload)
            if check is False:
                break

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
            last_value = self.get_last_value_schema(
                json_fields[i], self.original_object
            )
            # Check if last value is a json object
            if isinstance(last_value, dict):
                # If it is, make sure it does not have any subfields
                last_value_type = last_value.get("type")
                if last_value_type == "object" or last_value_type == "array":
                    to_remove.append(i)
        for to_rem_item in sorted(to_remove, reverse=True):
            del original_json_fields[to_rem_item]
        return original_json_fields

    def get_last_value_schema(self, field_list, last_object):
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
            last_value = self.get_last_value_schema(field_list, new_object)
        # If the new last current object cannot be found via the path, the path is wrong
        elif not new_object:
            logging.error(
                f"Object {last_object} does not contain field {new_field}, field path is wrong"
            )
            sys.exit(1)
        return last_value

    def get_last_value_object(self, field_list, last_object, fields_until_now):
        # Get first field in path of fields
        new_field = field_list.pop(0)
        # Check if object is list according to schema
        value_in_schema = self.get_last_value_schema(
            fields_until_now, self.original_object
        )
        fields_until_now.append(new_field)
        type_of_field = value_in_schema.get("type")
        if type_of_field == "array":
            last_value = []
            for lo in last_object:
                # Check if field_list is empty because that is the end of the object
                if not field_list:
                    last_value.append(lo[new_field])
                else:
                    last_object = lo[new_field]
                    last_value.append(
                        self.get_last_value_object(field_list, lo, fields_until_now)
                    )
        else:
            # Check if field_list is empty because that is the end of the object
            if not field_list:
                last_value = last_object[new_field]
            else:
                last_object = last_object[new_field]
                last_value = self.get_last_value_object(
                    field_list, last_object, fields_until_now
                )
        return last_value

    def clean_message(self, schema_field_lists, message, message_depth):  # noqa: C901
        # Check if the message is a list because only objects can be checked
        if isinstance(message, list):
            for m in message:
                self.clean_message(schema_field_lists, m, message_depth)
        else:
            # For every key in the message
            for key in list(message.keys()):
                if key in FIELDS_TO_SKIP:
                    continue
                in_list = False
                # Check if the key can be found in the list with schema fields and if it has the right depth
                for schema_field_list in schema_field_lists:
                    for i in range(len(schema_field_list)):
                        if i == message_depth and schema_field_list[i] == key:
                            in_list = True
                            break
                    if in_list is True:
                        break
                # If it cannot be found in the schema fields list, delete it
                if in_list is False:
                    del message[key]
                # If it can, check if the key has an object or list as value
                # Because if it does, the value needs to be checked as well
                if key in message:
                    if isinstance(message[key], list) or isinstance(message[key], dict):
                        self.clean_message(
                            schema_field_lists, message[key], message_depth + 1
                        )

    def check_for_missing_values_message(self, schema_field_list, message):
        good_message = True
        # For every schema field
        for i in range(len(schema_field_list)):
            # Check if the message is a list
            if isinstance(message, list):
                # If it is a list, recursively call the function with every object in the list
                for m in message:
                    good_message = self.check_for_missing_values_message(
                        schema_field_list[i:], m
                    )
                    if good_message is False:
                        break
            # If it is not a list
            else:
                # Check if the schema field can be found in the message
                if schema_field_list[i] not in message:
                    good_message = False
                    break
                else:
                    message = message[schema_field_list[i]]
        return good_message

    def copy_list_of_lists(self, list_to_copy):
        copied_list = []
        for a_list in list_to_copy:
            a_list_copy = []
            for value in a_list:
                a_list_copy.append(value)
            copied_list.append(a_list_copy)
        return copied_list
