import os
import asyncio
import time
import pandas as pd
from TikTokApi import TikTokApi
from TikTokApi.exceptions import EmptyResponseException, CaptchaException

async def get_tiktok_data(video_url: str, api: TikTokApi) -> int:
    try:
        video = api.video(url=video_url)
        data = await video.info()
        return data.get("stats", {}).get("playCount", 0)
    except (EmptyResponseException, CaptchaException) as e:
        print(f"TikTok Blocked: {str(e)}")
        return None
    except Exception as e:
        print(f"General Error: {str(e)}")
        return None

async def process_urls(urls: list, api: TikTokApi) -> list:
    results = []
    for index, url in enumerate(urls):
        print(f"Processing ({index+1}/{len(urls)}): {url}")
        try:
            views = await get_tiktok_data(url, api)
            results.append(views)
        except Exception as e:
            print(f"Fatal error: {str(e)}")
            results.append(None)
        await asyncio.sleep(1)
    return results

async def main():
    try:
        
        df = pd.read_excel("urls.xlsx")
        
      
        URL_COL = 8   
        VIEWS_COL = 10  
        
       
        if df.shape[1] <= max(URL_COL, VIEWS_COL):
            print("Excel file missing required columns")
            return
            
        
        valid_entries = []
        for idx, row in df.iterrows():
            url = row.iloc[URL_COL]
            if pd.notna(url) and str(url).strip():
                valid_entries.append((idx, str(url).strip()))
                
        urls = [url for idx, url in valid_entries]
        
    except FileNotFoundError:
        print("Create urls.xlsx first!")
        return

    if not urls:
        print("No valid URLs found")
        return

    print(f"Starting scrape of {len(urls)} URLs...")
    start = time.time()
    
    async with TikTokApi() as api:
        await api.create_sessions(
            num_sessions=1,
            headless=True,
            ms_tokens=[os.getenv("MS_TOKEN")],
            sleep_after=3
        )
        results = await process_urls(urls, api)
    
    # Update views 
    for i, views in enumerate(results):
        if views is not None:
            row_idx = valid_entries[i][0]
            df.iloc[row_idx, VIEWS_COL] = views
    
    # Save update data
    df.to_excel("urls.xlsx", index=False)
    print(f"Successfully updated {len(results)} entries")
    print(f"Completed in {time.time()-start:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())