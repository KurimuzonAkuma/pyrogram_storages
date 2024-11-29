# Multisession postgres storage

Author: PureAholy

## Example

```python
from storage import MultiPostgresStorage

async def main():
    workdir = Path(__file__).parent

    database = {
        'db_user': 'postgres',
        'db_pass': 'password',
        'db_host': 'localhost',
        'db_port': 5432,
        'db_name': 'pyrogram'
    }

    app = Client(
        session_name,
        api_id=api_id,
        api_hash=api_hash
    )
    app.storage = MultiPostgresStorage(client=app, database=database)

    await app.start()

    print(await app.get_me())

    await app.stop()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```
