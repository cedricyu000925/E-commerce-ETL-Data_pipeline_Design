"""
Transform Products into Dimension Table
Includes category translation and grouping
Uses Pandas for string operations
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.extract.extract_products import ProductsExtractor

logger = setup_logger('transform_dim_products')


class ProductDimensionBuilder:
    """Build Product Dimension with category enrichment"""
    
    def __init__(self):
        """Initialize product dimension builder"""
        # Product category translation (Portuguese to English)
        self.category_translation = {
            'beleza_saude': 'health_beauty',
            'informatica_acessorios': 'computers_accessories',
            'automotivo': 'automotive',
            'cama_mesa_banho': 'bed_bath_table',
            'moveis_decoracao': 'furniture_decor',
            'esporte_lazer': 'sports_leisure',
            'perfumaria': 'perfumery',
            'utilidades_domesticas': 'housewares',
            'telefonia': 'telephony',
            'relogios_presentes': 'watches_gifts',
            'alimentos_bebidas': 'food_drinks',
            'bebes': 'baby',
            'papelaria': 'stationery',
            'tablets_impressao_imagem': 'tablets_printing_image',
            'brinquedos': 'toys',
            'telefonia_fixa': 'fixed_telephony',
            'ferramentas_jardim': 'garden_tools',
            'fashion_bolsas_e_acessorios': 'fashion_bags_accessories',
            'eletrônicos': 'electronics',
            'eletrodomesticos': 'home_appliances',
            'livros_interesse_geral': 'books_general_interest',
            'construcao_ferramentas_construcao': 'construction_tools_construction',
            'casa_construcao': 'home_construction',
            'instrumentos_musicais': 'musical_instruments',
            'eletrodomesticos_2': 'home_appliances_2',
            'livros_tecnicos': 'books_technical',
            'cool_stuff': 'cool_stuff',
            'malas_acessorios': 'luggage_accessories',
            'climatizacao': 'air_conditioning',
            'construcao_ferramentas_iluminacao': 'construction_tools_lighting',
            'artigos_de_festas': 'party_supplies',
            'construcao_ferramentas_seguranca': 'construction_tools_safety',
            'industria_comercio_e_negocios': 'industry_commerce_business',
            'livros_importados': 'books_imported',
            'pcs': 'computers',
            'artigos_de_natal': 'christmas_articles',
            'fashion_calcados': 'fashion_shoes',
            'flores': 'flowers',
            'artes_e_artesanato': 'arts_crafts',
            'fraldas_higiene': 'diapers_hygiene',
            'fashion_underwear_e_moda_praia': 'fashion_underwear_beach',
            'pet_shop': 'pet_shop',
            'moveis_sala': 'living_room_furniture',
            'construcao_ferramentas_jardim': 'construction_tools_garden',
            'fashion_esporte': 'fashion_sports',
            'sinalizacao_e_seguranca': 'signaling_security',
            'la_cuisine': 'la_cuisine',
            'dvds_blu_ray': 'dvds_blu_ray',
            'fashion_roupa_masculina': 'fashion_male_clothing',
            'portateis_casa_forno_e_cafe': 'portable_kitchen_food_processor',
            'cds_dvds_musicais': 'cds_dvds_musicals',
            'consoles_games': 'consoles_games',
            'audio': 'audio',
            'fashion_roupa_feminina': 'fashion_female_clothing',
            'seguros_e_servicos': 'insurance_services',
            'portateis_cozinha_e_preparadores_de_alimentos': 'portable_kitchen',
            'casa_conforto_2': 'home_comfort_2',
            'agro_industria_e_comercio': 'agro_industry_commerce',
            'market_place': 'market_place',
            'fashion_roupa_infanto_juvenil': 'fashion_children_clothes',
            'musica': 'music',
            'casa_conforto': 'home_comfort',
            'cine_foto': 'cine_photo',
            'moveis_cozinha_area_de_servico_jantar_e_jardim': 'kitchen_dining_laundry_garden_furniture',
            'moveis_escritorio': 'office_furniture',
            'moveis_quarto': 'bedroom_furniture',
            'fashion_roupa_de_banho': 'fashion_swimwear',
            'alimentos': 'food',
            'artes': 'arts',
            'eletronicos': 'electronics',
            'livros': 'books'
        }
        
        # Category grouping into segments
        self.category_segments = {
            'Electronics': [
                'computers_accessories', 'telephony', 'electronics', 
                'tablets_printing_image', 'fixed_telephony', 'computers',
                'consoles_games', 'audio', 'cine_photo'
            ],
            'Home & Furniture': [
                'bed_bath_table', 'furniture_decor', 'housewares', 
                'home_appliances', 'home_construction', 'air_conditioning',
                'living_room_furniture', 'kitchen_dining_laundry_garden_furniture',
                'office_furniture', 'bedroom_furniture', 'home_comfort',
                'home_appliances_2', 'home_comfort_2', 'la_cuisine'
            ],
            'Fashion & Beauty': [
                'health_beauty', 'perfumery', 'fashion_bags_accessories',
                'fashion_shoes', 'fashion_underwear_beach', 'fashion_sports',
                'fashion_male_clothing', 'fashion_female_clothing',
                'fashion_children_clothes', 'fashion_swimwear', 'watches_gifts'
            ],
            'Sports & Leisure': [
                'sports_leisure', 'toys', 'baby', 'pet_shop', 'diapers_hygiene'
            ],
            'Books & Media': [
                'books_general_interest', 'books_technical', 'books_imported',
                'dvds_blu_ray', 'cds_dvds_musicals', 'music', 'stationery',
                'arts_crafts', 'arts'
            ],
            'Automotive & Tools': [
                'automotive', 'garden_tools', 'construction_tools_construction',
                'construction_tools_lighting', 'construction_tools_safety',
                'construction_tools_garden', 'signaling_security'
            ],
            'Food & Drinks': [
                'food_drinks', 'food', 'portable_kitchen_food_processor',
                'portable_kitchen'
            ],
            'Gifts & Party': [
                'party_supplies', 'christmas_articles', 'flowers', 'cool_stuff'
            ],
            'Business & Industry': [
                'industry_commerce_business', 'agro_industry_commerce',
                'office_furniture', 'musical_instruments'
            ],
            'Services': [
                'insurance_services', 'market_place'
            ],
            'Other': []  # Catch-all for uncategorized
        }
        
        logger.info("ProductDimensionBuilder initialized")
    
    def build(self):
        """
        Build product dimension with enriched categories
        
        Returns:
            DataFrame: Product dimension (Pandas)
        """
        try:
            logger.info("Building product dimension...")
            
            # Extract products
            extractor = ProductsExtractor()
            df = extractor.extract()
            
            logger.info(f"✓ Loaded {len(df):,} products")
            
            # Handle missing product_id (should not happen, but defensive coding)
            if df['product_id'].isnull().any():
                logger.warning("⚠ Removing products with null product_id")
                df = df[df['product_id'].notna()]
            
            # Translate category names
            df['product_category_english'] = df['product_category_name'].map(
                self.category_translation
            )
            
            # Fill missing translations with original name or 'Uncategorized'
            missing_translations = df['product_category_english'].isnull()
            if missing_translations.any():
                logger.warning(f"⚠ {missing_translations.sum()} categories without translation")
                df.loc[missing_translations, 'product_category_english'] = df.loc[
                    missing_translations, 'product_category_name'
                ].fillna('uncategorized')
            
            logger.info("✓ Translated product categories to English")
            
            # Assign category segments
            df['product_category_segment'] = df['product_category_english'].apply(
                self._get_category_segment
            )
            
            logger.info("✓ Assigned category segments")
            
            # Log segment distribution
            segment_dist = df['product_category_segment'].value_counts()
            logger.info("  - Category segment distribution:")
            for segment, count in segment_dist.items():
                logger.info(f"    → {segment}: {count:,} products")
            
            # Ensure product_volume_cm3 exists (calculated in extract)
            if 'product_volume_cm3' not in df.columns:
                dim_cols = ['product_length_cm', 'product_height_cm', 'product_width_cm']
                if all(col in df.columns for col in dim_cols):
                    df['product_volume_cm3'] = (
                        df['product_length_cm'] * 
                        df['product_height_cm'] * 
                        df['product_width_cm']
                    )
            
            # Ensure has_photos flag exists
            if 'has_photos' not in df.columns and 'product_photos_qty' in df.columns:
                df['has_photos'] = df['product_photos_qty'] > 0
            
            # Create surrogate key (auto-increment integer)
            df = df.reset_index(drop=True)
            df.insert(0, 'product_key', df.index + 1)
            
            # Add timestamp
            df['created_at'] = pd.Timestamp.now()
            
            # Select and order final columns
            final_columns = [
                'product_key',
                'product_id',
                'product_category_name',
                'product_category_english',
                'product_category_segment',
                'product_weight_g',
                'product_length_cm',
                'product_height_cm',
                'product_width_cm',
                'product_volume_cm3',
                'product_photos_qty',
                'has_photos',
                'created_at'
            ]
            
            # Keep only columns that exist
            final_columns = [col for col in final_columns if col in df.columns]
            df = df[final_columns]
            
            logger.info(f"✓ Product dimension built successfully: {len(df):,} rows")
            logger.info(f"  - Columns: {len(df.columns)}")
            logger.info(f"  - Category segments: {df['product_category_segment'].nunique()}")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Product dimension build failed: {e}")
            raise
    
    def _get_category_segment(self, category_english):
        """Map category to segment"""
        for segment, categories in self.category_segments.items():
            if category_english in categories:
                return segment
        return 'Other'


# Test the builder
if __name__ == "__main__":
    builder = ProductDimensionBuilder()
    df_products = builder.build()
    
    print("\n" + "="*80)
    print("PRODUCT DIMENSION TEST")
    print("="*80)
    
    print(f"\nTotal products: {len(df_products):,}")
    print(f"\nColumns: {list(df_products.columns)}")
    
    print("\nFirst 10 rows:")
    print(df_products.head(10))
    
    print("\nData types:")
    print(df_products.dtypes)
    
    print("\nCategory segment distribution:")
    print(df_products['product_category_segment'].value_counts())
    
    print("\nSample products by segment:")
    for segment in df_products['product_category_segment'].unique()[:3]:
        print(f"\n{segment}:")
        sample = df_products[df_products['product_category_segment'] == segment].head(3)
        print(sample[['product_key', 'product_category_english', 'product_category_segment']])
