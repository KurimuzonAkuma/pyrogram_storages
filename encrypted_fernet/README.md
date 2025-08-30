# Encrypted Fernet

Thanks to PureAholy

> [!WARNING]
> This storage encrypts ***only*** the auth key.
> 
> So if someone has your session file, they will not be able to access your account without the key.

## Example

```python
from storage import EncryptedFernetStorage

async def main():
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
