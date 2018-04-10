# tap-mailchimp-export

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from (http:/mailchimp.com)
- Extracts the following resources:
  - Campaigns
  - Campaign Subscriber Activity
  - Lists
  - List Members
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2018 Simon Data
