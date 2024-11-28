# Aiosqlite

## Example

```python
from storage import AIOSQLiteStorage

async def main():
    workdir = Path(__file__).parent

    app = Client(
        session_name,
        api_id=api_id,
        api_hash=api_hash
    )
    app.storage = AIOSQLiteStorage(client=app)

    await app.start()

    print(await app.get_me())

    await app.stop()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```
