import asyncio
import os
import sys

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from backend.agent import BIAgent
from dotenv import load_dotenv

async def run_test():
    load_dotenv()
    
    agent = BIAgent(
        groq_key=os.environ.get("GROQ_API_KEY"),
        monday_key=os.environ.get("MONDAY_API_KEY"),
        deals_board_id=os.environ.get("DEALS_BOARD_ID"),
        wo_board_id=os.environ.get("WORKORDERS_BOARD_ID"),
    )
    
    try:
        print("Sending query to agent: 'How is our pipeline looking?'\n")
        result = await agent.query("How is our pipeline looking?", [])
        with open("result.txt", "w", encoding="utf-8") as f:
            f.write("--- Answer ---\n")
            f.write(str(result.get("answer")))
    except Exception as e:
        import traceback
        with open("result.txt", "w", encoding="utf-8") as f:
            f.write("--- Error ---\n")
            f.write(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(run_test())
