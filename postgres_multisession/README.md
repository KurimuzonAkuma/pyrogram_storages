# Multisession postgres storage

Author: PureAholy

## Example

```python
from storage import MultiPostgresStorage

async def main():
    workdir = Path(__file__).parent

    pool = await asyncpg.create_pool(
        user='postgres',
        password='password',
        database='pyrogram',
        host='localhost',
        port=5432,
    )

    app = Client(
        session_name,
        api_id=api_id,
        api_hash=api_hash
    )
    app.storage = MultiPostgresStorage(client=app, pool=pool)

    await app.start()

    print(await app.get_me())

    await app.stop()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```
