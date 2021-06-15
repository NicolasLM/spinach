import aiohttp
from spinach import Engine, MemoryBroker, Batch, AsyncioWorkers

spin = Engine(MemoryBroker())


@spin.task(name='get_pokemon_name')
async def get_pokemon_name(pokemon_id: int):
    """Call an HTTP API to retrieve a pokemon name by its ID."""
    url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            pokemon = await response.json()

    print(f'Pokemon #{pokemon_id} is {pokemon["name"]}')


# Schedule a batch of 150 tasks to retrieve the name of the
# first 150 pokemons.
batch = Batch()
for pokemon_id in range(1, 151):
    batch.schedule(get_pokemon_name, pokemon_id)
spin.schedule_batch(batch)

# Start the asyncio workers and process the tasks
spin.start_workers(
    number=256,
    workers_class=AsyncioWorkers,
    stop_when_queue_empty=True
)
