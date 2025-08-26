import pandas as pd
from textblob import TextBlob
import re

class DataProcessor:
    """
    Clase para limpiar, procesar y enriquecer los datos de videos de YouTube.
    """
    def __init__(self, input_path: str):
        """
        Inicializa el procesador.
        Args:
            input_path (str): Ruta al archivo CSV con los datos crudos de videos.
        """
        try:
            self.df = pd.read_csv(input_path)
        except FileNotFoundError:
            print(f"Error: No se encontró el archivo en la ruta: {input_path}")
            self.df = None

    def clean_data(self):
        """Realiza la limpieza básica de los datos."""
        if self.df is None: return self
        
        # Convertir a datetime
        self.df['publishedAt'] = pd.to_datetime(self.df['publishedAt'], errors='coerce')
        # Eliminar filas donde el título es nulo
        self.df.dropna(subset=['title'], inplace=True)
        # Asegurar que las métricas son numéricas
        numeric_cols = ['viewCount', 'likeCount', 'commentCount']
        for col in numeric_cols:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
        
        return self

    def create_features(self):
        """Crea nuevas características (features) para el análisis."""
        if self.df is None: return self

        # Tasa de Interacción (Engagement Rate)
        self.df['engagement_rate'] = (
            (self.df['likeCount'] + self.df['commentCount']) / (self.df['viewCount'] + 1)
        ) * 100
        
        return self

    def analyze_sentiment(self):
        """
        Añade una columna con la clasificación del sentimiento de los títulos.
        """
        if self.df is None: return self

        def get_sentiment_polarity(text: str) -> float:
            """Retorna la polaridad del sentimiento (-1 a 1)."""
            return TextBlob(str(text)).sentiment.polarity

        def classify_sentiment(polarity: float) -> str:
            """Clasifica la polaridad en Positivo, Negativo o Neutro."""
            if polarity > 0.1: # Umbral para considerarlo positivo
                return "Positivo"
            elif polarity < -0.1: # Umbral para considerarlo negativo
                return "Negativo"
            else:
                return "Neutro"
        
        # Aplicar ambas funciones
        self.df['sentiment_polarity'] = self.df['title'].apply(get_sentiment_polarity)
        self.df['sentiment_label'] = self.df['sentiment_polarity'].apply(classify_sentiment)
        
        return self

    def process_and_save(self, output_path: str):
        """
        Ejecuta todo el pipeline de procesamiento y guarda el resultado.
        Args:
            output_path (str): Ruta para guardar el CSV procesado.
        """
        if self.df is None:
            print("No hay datos para procesar.")
            return None
        
        # Ejecutar el pipeline
        self.clean_data()
        self.create_features()
        self.analyze_sentiment()
        
        # Guardar el archivo final
        self.df.to_csv(output_path, index=False)
        print(f"Procesamiento completado. Datos guardados en: {output_path}")
        return self.df


if __name__ == '__main__':
    # --- Rutas de archivos ---
    INPUT_PATH = "data/raw_videos.csv"
    OUTPUT_PATH = "data/videos_final_con_sentimiento.csv"

    # --- Ejecución ---
    processor = DataProcessor(input_path=INPUT_PATH)
    processed_df = processor.process_and_save(output_path=OUTPUT_PATH)
    
    if processed_df is not None:
        print("\\n--- Muestra de los datos finales ---")
        print(processed_df[['title', 'engagement_rate', 'sentiment_label']].head())