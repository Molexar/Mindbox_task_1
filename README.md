## Testing task # 1 to Junior Mindbox Data Engineer

### All solution contains at `set_sessions` method in `main.py` that create column session with unique ids

### File `main.py` has `df` variable with fake data that can be replaced with real data or readed from csv

### Algorithm described in helping method `application`.
### Briefly algorithm contains from:
- gropuby dataframe by customer_id
- sort rows by timestamp
- iterate over timestamp and setting unique session keys

### O-notation of algorithm: `O(n + n * log(n))` without calculating groupping by keys