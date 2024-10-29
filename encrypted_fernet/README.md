# Encrypted Fernet

## Example

```python
from storage import EncryptedFernetStorage

async def main():
    workdir = Path(__file__).parent
    test_mode = True

    app = Client(
        session_name,
        api_id=api_id,
        api_hash=api_hash
    )
    app.storage = EncryptedFernetStorage(client=app, key=b"my_secret_key")

    await app.start()

    print(await app.get_me())

    await app.stop()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```
