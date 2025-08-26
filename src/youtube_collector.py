import os
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

class YouTubeCollector:
    """
    Clase para recolectar datos de videos de múltiples canales de YouTube.
    """
    def __init__(self, api_key: str):
        """
        Inicializa el recolector.
        Args:
            api_key (str): La clave de la API de YouTube.
        """
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)

    def get_channel_stats(self, channel_ids: list) -> pd.DataFrame:
        """Obtiene las estadísticas de una lista de canales."""
        all_data = []
        request = self.youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=",".join(channel_ids)
        )
        response = request.execute()

        for item in response['items']:
            data = {
                'channel_name': item['snippet']['title'],
                'channel_id': item['id'],
                'subscribers': int(item['statistics']['subscriberCount']),
                'views': int(item['statistics']['viewCount']),
                'total_videos': int(item['statistics']['videoCount']),
                'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
            }
            all_data.append(data)
        return pd.DataFrame(all_data)

    def get_all_videos_from_channels(self, channels_df: pd.DataFrame) -> pd.DataFrame:
        """Obtiene todos los detalles de los videos de los canales proporcionados."""
        all_videos_df = pd.DataFrame()

        for index, row in channels_df.iterrows():
            print(f"Recolectando videos para el canal: {row['channel_name']}...")
            playlist_id = row['playlist_id']
            video_ids = self._get_video_ids_from_playlist(playlist_id)
            video_details_df = self._get_video_details(video_ids)
            video_details_df['channel_name'] = row['channel_name'] # Añadir nombre del canal
            all_videos_df = pd.concat([all_videos_df, video_details_df], ignore_index=True)
        
        return all_videos_df

    def _get_video_ids_from_playlist(self, playlist_id: str) -> list:
        """Función auxiliar para obtener los IDs de un playlist."""
        video_ids = []
        next_page_token = None
        
        while True:
            request = self.youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            video_ids.extend([item['contentDetails']['videoId'] for item in response['items']])
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        return video_ids

    def _get_video_details(self, video_ids: list) -> pd.DataFrame:
        """Función auxiliar para obtener detalles de videos en lotes de 50."""
        all_video_stats = []
        for i in range(0, len(video_ids), 50):
            request = self.youtube.videos().list(
                part='snippet,statistics',
                id=','.join(video_ids[i:i+50])
            )
            response = request.execute()
            for video in response['items']:
                stats = {
                    'title': video['snippet']['title'],
                    'publishedAt': video['snippet']['publishedAt'],
                    'viewCount': int(video['statistics'].get('viewCount', 0)),
                    'likeCount': int(video['statistics'].get('likeCount', 0)),
                    'commentCount': int(video['statistics'].get('commentCount', 0)),
                }
                all_video_stats.append(stats)
        return pd.DataFrame(all_video_stats)


if __name__ == '__main__':
    # --- Configuración ---
    API_KEY = os.getenv("YOUTUBE_API_KEY") # Clave segura desde .env
    CHANNEL_IDS = [
        'UC5mQQ4Ur6oA8lsGFkvH2lyg', # Noche De Chicxs
        'UC2UXDak6o7rBm23k3Vv5dww', # Tina Huang
        'UCrDytBh6F_rJ8vi2nPtIQeA', # Clara Carmona
        'UCevIn3faqGtFlMucnrCINHA', # Kristoff Raczynski
        'UC8t7S1o1dzvr9j1_idpVTSQ'  # Liry Onni
    ]
    OUTPUT_CHANNELS_PATH = "data/channels.csv"
    OUTPUT_VIDEOS_PATH = "data/raw_videos.csv"

    # --- Ejecución ---
    if not API_KEY:
        print("Error: La variable de entorno YOUTUBE_API_KEY no está configurada.")
    else:
        collector = YouTubeCollector(api_key=API_KEY)
        
        # Recolectar y guardar datos de canales
        channels_df = collector.get_channel_stats(CHANNEL_IDS)
        channels_df.to_csv(OUTPUT_CHANNELS_PATH, index=False)
        print(f"Datos de canales guardados en: {OUTPUT_CHANNELS_PATH}")
        
        # Recolectar y guardar datos de videos
        videos_df = collector.get_all_videos_from_channels(channels_df)
        videos_df.to_csv(OUTPUT_VIDEOS_PATH, index=False)
        print(f"Datos de videos guardados en: {OUTPUT_VIDEOS_PATH}")