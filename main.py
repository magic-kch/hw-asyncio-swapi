import asyncio
import datetime
import requests
import aiohttp
import more_itertools
import tqdm


from models import Session, SwapiPeople, close_orm, init_orm
URL = "https://swapi.py4e.com/api/people/"
MAX_COROS = 10

def get_people_count(url):
    response = requests.get(url)
    json_data = response.json()
    return json_data['count']


async def get_people(person_id, http_session):
    url = f"https://swapi.py4e.com/api/people/{person_id}/"
    http_response = await http_session.get(url)
    json_data = await http_response.json()
    return json_data


async def from_list_to_string(list_of_urls: list | str):
    if 'error' in list_of_urls:
        return 'error'
    async with aiohttp.ClientSession() as session:
        res_list = []
        for url in list_of_urls:
            async with session.get(url) as response:
                json_data = await response.json()
                res_list.append(json_data.get('name', json_data.get('title')))
        return ', '.join(res_list)


async def insert_people(json_list: list[dict]):
    async with Session() as session:
        swapi_people_list = []
        for item in json_list:
            swapi_person = SwapiPeople(
                birth_year=item.get('birth_year', 'error'),
                eye_color=item.get('eye_color', 'error'),
                gender=item.get('gender', 'error'),
                hair_color=item.get('hair_color', 'error'),
                height=item.get('height', 'error'),
                homeworld=await from_list_to_string([item.get('homeworld', 'error'),]),
                mass=item.get('mass', 'error'),
                name=item.get('name', 'error'),
                skin_color=item.get('skin_color', 'error'),
                films=await from_list_to_string(item.get('films', 'error')),
                species=await from_list_to_string(item.get('species', 'error')),
                starships=await from_list_to_string(item.get('starships', 'error')),
                vehicles=await from_list_to_string(item.get('vehicles', 'error')),

            )
            swapi_people_list.append(swapi_person)

        session.add_all(swapi_people_list)
        await session.commit()

async def main():
    await init_orm()

    async with aiohttp.ClientSession() as http_session:
        for i_list in more_itertools.chunked(range(1, get_people_count(URL)), MAX_COROS):
            coros = [get_people(i, http_session) for i in i_list]
            result = await asyncio.gather(*coros)
            coro = insert_people(result)
            asyncio.create_task(coro)

        tasks = asyncio.all_tasks()

        task_main = asyncio.current_task()
        tasks.remove(task_main)

        await asyncio.gather(*tasks)

    await close_orm()

async def main_with_progress():
    await init_orm()

    async with aiohttp.ClientSession() as http_session:
        total = get_people_count(URL)
        with tqdm.tqdm(total=total, desc="Progress") as pbar:
            for i_list in more_itertools.chunked(range(1, total), MAX_COROS):
                coros = [get_people(i, http_session) for i in i_list]
                result = await asyncio.gather(*coros)
                coro = insert_people(result)
                asyncio.create_task(coro)

                pbar.update(len(i_list))

            tasks = asyncio.all_tasks()

            task_main = asyncio.current_task()
            tasks.remove(task_main)

            await asyncio.gather(*tasks)

            pbar.update(total - pbar.n)

    await close_orm()


asyncio.run(main())


# asyncio.run(main())
asyncio.run(main_with_progress())
print('База данных заполнена')
