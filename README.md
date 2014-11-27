#### run app locally 

unless you do [this](https://gist.github.com/pwalsh/b8563e1a1de3347a8066), you wont be able to run queries against BigQuery locally on your machine

once you've done the above, run this:

```
dev_appserver.py --appidentity_email_address XXXXXXXXXXXXXXXX@developer.gserviceaccount.com --appidentity_private_key_path /Users/ADMIN/Downloads/secret.pem /Users/ADMIN/Documents/gae-python/gae-py-bq/
```