# import json
import logging
import sys

from config import FIELDS_TO_SKIP

# from gobits import Gobits
# from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)


class MessageProcessor(object):
    def __init__(self):
        print("joe")

    def process(self, payload):
        # Get schema
        # TODO: replace with schema API
        schema = "schema.json"
        # Get properties of schema
        properties = schema.get("properties")
        if not properties:
            sys.exit(0)
        # Makes lists of fields in schema
        current_field_list, schema_field_list = self.list_of_schema_fields(
            properties, [], []
        )
        print(schema_field_list)

    #     message = payload["message"]
    #     if self.process_message(message) is False:
    #         logging.info("Message not processed")
    #     else:
    #         logging.info("Message is processed")

    # def process_message(self, message):
    #     return True

    def list_of_schema_fields(self, json_object, current_field_list, current_list):
        # print("")
        if isinstance(json_object, dict):
            type_object = json_object.get("type")
            if type_object == "array":
                json_object = json_object["items"]
                # print(json_object)
                current_field_list, current_list = self.list_of_schema_fields(
                    json_object, current_field_list, current_list
                )
            elif type_object == "object":
                json_object = json_object["properties"]
                # print(json_object)
                current_field_list, current_list = self.list_of_schema_fields(
                    json_object, current_field_list, current_list
                )
            elif not type_object:
                # print(json_object)
                current_field_list_copy = []
                for cf in current_field_list:
                    current_field_list_copy.append(cf)
                for field in json_object:
                    if field in FIELDS_TO_SKIP:
                        continue
                    current_field_list.append(field)
                    current_field_list, current_list = self.list_of_schema_fields(
                        json_object[field], current_field_list, current_list
                    )
                    if isinstance(json_object[field], dict):
                        if (
                            json_object[field].get("type") != "array"
                            or json_object[field].get("type") != "object"
                        ):
                            current_list.append(current_field_list)
                            current_field_list = []
                            current_field_list.extend(current_field_list_copy)
                    else:
                        current_list.append(current_field_list)
                        current_field_list = []
                        current_field_list.extend(current_field_list_copy)
        return current_field_list, current_list
