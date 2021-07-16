# Consume for Projection
This function consumes messages posted on a Pub/Sub Topic and puts them in a database based on the schema of the topic.

## Setup
1. Make sure a ```config.py``` file exists within the directory, based on the [config.example.py](config.py.example), with the correct configuration:
    ~~~
    DEBUG_LOGGING = Set this to True if you want the debugging logging to show
    FIELDS_TO_SKIP = Set this as a list with fields from the schema that should not be in database
    ~~~
2. Make sure the following values are set in the environment variables:
    ~~~
    TOPIC_NAME = Set this to True if you want the debugging logging to show
    SCHEMA_FILE_PATH = Set this as a list with fields from the schema that should not be in database
    ~~~
3. Deploy the function with help of the [cloudbuild.example.yaml](cloudbuild.yaml.example) to the Google Cloud Platform.

## Incoming message
To make sure the function works according to the way it was intented, the incoming messages from a Pub/Sub Topic must have the following structure based on the [company-data structure](https://vwt-digital.github.io/project-company-data.github.io/v1.1/schema):
~~~JSON
{
  "gobits": [ ],
  "root_field": {
    "field_1": "value_1",
    "field_2": "value_2",
    "field_etcetera": "value_etcetera"
  }
}
~~~

or

~~~JSON
{
  "gobits": [ ],
  "field_1": "value_1",
  "field_2": "value_2",
  "field_etcetera": "value_etcetera"
}
~~~


## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License
