from flask import Flask, render_template, request, jsonify
import threading
import time
import logging
from youtube_scraper import YouTubeScraper

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global variables to store scraping results
scraping_results = {
    'channels': [],
    'video': None,
    'status': 'idle',
    'error': None
}

def scrape_channels():
    """Background function to scrape channel data"""
    global scraping_results
    scraping_results['status'] = 'scraping_channels'
    scraping_results['error'] = None
    
    try:
        scraper = YouTubeScraper()
        
        # Process the required channels
        channels = {
            "iNeuron": "https://www.youtube.com/@iNeuroniNtelligence",
            "Krish Naik": "https://www.youtube.com/@krishnaik06",
            "College Wallah": "https://www.youtube.com/@CollegeWallahbyPW"
        }
        
        results = []
        for name, url in channels.items():
            try:
                result = scraper.process_channel(name, url)
                results.append(result)
                time.sleep(2)  # Be polite with requests
            except Exception as e:
                logger.error(f"Error processing channel {name}: {str(e)}")
                results.append({
                    'name': name,
                    'url': url,
                    'video_count': 0,
                    'error': str(e)
                })
        
        scraping_results['channels'] = results
        scraping_results['status'] = 'channels_complete'
        scraper.close()
        
    except Exception as e:
        scraping_results['status'] = 'error'
        scraping_results['error'] = str(e)
        logger.error(f"Error in channel scraping: {str(e)}")

def scrape_video(video_url):
    """Background function to scrape video data"""
    global scraping_results
    scraping_results['status'] = 'scraping_video'
    scraping_results['error'] = None
    
    try:
        scraper = YouTubeScraper()
        video_data = scraper.process_video(video_url)
        
        scraping_results['video'] = video_data
        scraping_results['status'] = 'video_complete'
        scraper.close()
        
    except Exception as e:
        scraping_results['status'] = 'error'
        scraping_results['error'] = str(e)
        logger.error(f"Error in video scraping: {str(e)}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/scrape_channels', methods=['POST'])
def start_channel_scraping():
    if scraping_results['status'] not in ['idle', 'channels_complete', 'video_complete', 'error']:
        return jsonify({'status': 'busy', 'message': 'Scraping is already in progress'})
    
    # Start channel scraping in background thread
    thread = threading.Thread(target=scrape_channels)
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started', 'message': 'Channel scraping started'})

@app.route('/scrape_video', methods=['POST'])
def start_video_scraping():
    video_url = request.form.get('video_url')
    if not video_url:
        return jsonify({'status': 'error', 'message': 'No video URL provided'})
    
    if scraping_results['status'] not in ['idle', 'channels_complete', 'video_complete', 'error']:
        return jsonify({'status': 'busy', 'message': 'Scraping is already in progress'})
    
    # Start video scraping in background thread
    thread = threading.Thread(target=scrape_video, args=(video_url,))
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started', 'message': 'Video scraping started'})

@app.route('/status')
def get_status():
    return jsonify(scraping_results)

@app.route('/results')
def show_results():
    return render_template('results.html', 
                           channels=scraping_results['channels'], 
                           video=scraping_results['video'],
                           error=scraping_results['error'])

@app.route('/reset')
def reset_scraper():
    global scraping_results
    scraping_results = {
        'channels': [],
        'video': None,
        'status': 'idle',
        'error': None
    }
    return jsonify({'status': 'reset', 'message': 'Scraper reset successfully'})

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5000)