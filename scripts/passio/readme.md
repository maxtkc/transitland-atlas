```sh
http GET https://passiogo.com/mapGetData.php\?credentials\=1 | jq -r 'to_entries [].value.username'
```