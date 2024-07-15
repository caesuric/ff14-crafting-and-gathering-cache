1. make service for pulling data more reliable
2. process:
   1. client makes request with list of items
   2. server checks for cached items that have not expired
      1. returns any cached items
      2. expires the ones that are too old
   3. server requests all the other items (or max allowed paginating through them)
   4. server pulls as many as possible, caches data for them all and removes from queue
      1. cache for xivapi data is indefinite, cache for universalis data is like an hour maybe?
   5. server keeps pulling for missed items with a backaway and limited retry number
      1. if request is too big, server splits it in half and does half at a time
   6. server returns data for all items, in pages if needed
3. notes on process:
   1. should probably use a job system rather than a simple REST endpoint
   2. should be able to check on status of job for progress percentage
   3. interpolate during long requests using historical length of job based on number of items
   4. client should check job status once per second and display progress bar
4.
