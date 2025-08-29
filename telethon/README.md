# Telethon

## Example

```python
from storage import TelethonStorage

async def main():
    app = Client(
        session_name,
        api_id=api_id,
        api_hash=api_hash
    )
    app.storage = TelethonStorage(client=app)

    await app.start()

    print(await app.get_me())

    await app.stop()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```

# Pyrothon?

## Example

```python
from storage import TelethonStorage

async def main():
    app = Client(
        session_name,
        api_id=api_id,
        api_hash=api_hash
    )

    try:
        storage = TelethonStorage(client=client)
        await storage.open()
        await storage.close()

        app.storage = storage
    except sqlite3.OperationalError as e:
        pass

    await app.start()

    print(await app.get_me())

    await app.stop()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```
