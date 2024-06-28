# I2B2 Plugin

- I2B2 plugin flow is based on the manual installation steps as provided in the [i2b2 documentation](https://community.i2b2.org/wiki/display/getstarted/3.4+Crcdata+Tables).


## How to create I2B2 Dataset: 
- Trigger from jobs page e.g.
```
{
  "options": {
    "flow_action_type": "create_datamodel",
    "tag_name": "v1.8.0.0002",
    "schema_name": "testi2b2",
    "database_code": "alpdev_pg",
    "load_data": true
  }
}
```
- Refer to i2b2 tag name [here](https://github.com/i2b2/i2b2-data/releases)