# Example configuration for the LLM parser
# Add this to your config.py file
import os

# LLM Parser Configuration
LLM_PARSER_CONFIG = {
    "html": {
        "use_llm": True,
        "llm_config": {
            "api_key": "",  # Set via environment variable LLM_API_KEY
            "api_url": "https://api.openai.com/v1/chat/completions",  # Example for OpenAI
            "model": "gpt-4",  # Can be overridden with LLM_MODEL env var
            "max_tokens": 8000,  # Maximum tokens to process
            "temperature": 0.2,  # Lower temperature for more deterministic output
            "extraction_format": "json"  # Output format (json or csv)
        },
        "prompt_template": """
        You are an expert at extracting structured data from HTML content. 
        The following text is from the website of {source_desc}, which contains information about funded projects or grants.
        
        Please extract all the available project information into a structured JSON array format. 
        Each project should include fields like:
        - project_title
        - principal_investigator
        - institution
        - funding_amount (with currency if available)
        - funding_year
        - duration (if available)
        - description
        - research_area
        
        Include any other relevant fields you find. Use null for missing values.
        Return ONLY the JSON array with no additional text or explanation.
        
        Here's the content:
        {text_content}
        """
    }
}

# Update your PARSER_CONFIGS dictionary to include this:
PARSER_CONFIGS = {
    "standard_csv": {
        # Existing config...
    },
    "excel": {
        # Existing config...
    },
    "api": {
        # Existing config...
    },
    "html_scraper": {
        # Existing config...
    },
    "html": {
        "use_llm": True,
        "llm_config": LLM_PARSER_CONFIG["html"]["llm_config"],
        "prompt_template": LLM_PARSER_CONFIG["html"]["prompt_template"],
        "LLM_API_KEY": os.getenv("OPENAI_API_KEY"),  # Set via environment variable  
        "LLM_API_URL": os.getenv("LLM_API_URL", "https://api.openai.com/v1/chat/completions"),
        "LLM_MODEL": os.getenv("LLM_MODEL", "gpt-4o-mini"),  # Default to gpt-4
    }
}

# Required Environment Variables:
# - LLM_API_KEY: Your API key for the LLM provider
# - LLM_API_URL: The API endpoint for your LLM provider (defaults to OpenAI)
# - LLM_MODEL: The model to use (defaults to gpt-4)