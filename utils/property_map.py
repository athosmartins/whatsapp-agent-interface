"""
Property Map Visualization Component

This module creates interactive maps showing property locations and geometries
for owners based on their mega_data_set information.
"""

import pandas as pd
import streamlit as st
from typing import List, Dict, Any, Optional
import re

def get_available_map_styles() -> Dict[str, str]:
    """
    Get available map styles with their display names.
    Returns dict with style_key -> display_name mapping.
    """
    return {
        "OpenStreetMap": "🗺️ OpenStreetMap (Padrão)",
        "Satellite": "🛰️ Satélite",
        "Terrain": "🏔️ Terreno",
        "Streets": "🛣️ Ruas",
        "Physical": "🌍 Físico",
        "Light": "☀️ Claro",
        "Dark": "🌙 Escuro"
    }

def parse_wkt_multipolygon(wkt_string: str) -> List[List[List[float]]]:
    """
    Parse WKT MULTIPOLYGON string without using shapely.
    Returns coordinates as nested lists suitable for Folium.
    """
    try:
        # Clean up the WKT string
        wkt_string = str(wkt_string).strip()
        
        # Remove 'MULTIPOLYGON' prefix and surrounding spaces
        if wkt_string.startswith('MULTIPOLYGON'):
            coords_str = wkt_string[12:].strip()  # Remove 'MULTIPOLYGON'
        else:
            coords_str = wkt_string
        
        # Remove outer parentheses
        if coords_str.startswith('(') and coords_str.endswith(')'):
            coords_str = coords_str[1:-1].strip()
        
        polygons = []
        
        # Find all polygon groups: (((...)))
        polygon_pattern = r'\(\(\(([^)]+(?:\([^)]*\)[^)]*)*)\)\)\)'
        polygon_matches = re.findall(polygon_pattern, coords_str)
        
        if not polygon_matches:
            # Try simpler pattern for single polygon
            polygon_pattern = r'\(\(([^)]+)\)\)'
            polygon_matches = re.findall(polygon_pattern, coords_str)
        
        for match in polygon_matches:
            # Split coordinate pairs
            coord_pairs = match.split(',')
            polygon_coords = []
            
            for pair in coord_pairs:
                # Extract lat, lon from each pair
                coords = pair.strip().split()
                if len(coords) >= 2:
                    try:
                        lon = float(coords[0])
                        lat = float(coords[1])
                        polygon_coords.append([lat, lon])  # Folium expects [lat, lon]
                    except ValueError:
                        continue
            
            if polygon_coords:
                polygons.append(polygon_coords)
        
        # If still no matches, try direct coordinate extraction
        if not polygons:
            # Extract all coordinate pairs from the string
            coord_pattern = r'(-?\d+\.?\d*)\s+(-?\d+\.?\d*)'
            coord_matches = re.findall(coord_pattern, coords_str)
            
            if coord_matches:
                polygon_coords = []
                for lon_str, lat_str in coord_matches:
                    try:
                        lon = float(lon_str)
                        lat = float(lat_str)
                        polygon_coords.append([lat, lon])
                    except ValueError:
                        continue
                
                if polygon_coords:
                    polygons.append(polygon_coords)
        
        return polygons
        
    except Exception as e:
        print(f"Error parsing WKT: {e}")
        return []

def get_polygon_center(polygon_coords: List[List[float]]) -> Optional[List[float]]:
    """Calculate the center point of a polygon."""
    if not polygon_coords:
        return None
    
    try:
        lats = [coord[0] for coord in polygon_coords]
        lons = [coord[1] for coord in polygon_coords]
        
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        
        return [center_lat, center_lon]
    except:
        return None

def create_property_map(properties: List[Dict[str, Any]]) -> str:
    """
    Create an interactive map showing all properties for a person.
    Returns HTML string of the map.
    """
    try:
        import folium
        from folium.plugins import MarkerCluster
        
        if not properties:
            return "<p>Nenhuma propriedade encontrada.</p>"
        
        # Filter properties with valid geometry
        properties_with_geometry = []
        for prop in properties:
            geometry = prop.get('GEOMETRY')
            if geometry and pd.notna(geometry) and str(geometry).strip():
                properties_with_geometry.append(prop)
        
        if not properties_with_geometry:
            return "<p>Nenhuma propriedade com dados geográficos encontrada.</p>"
        
        print(f"Creating map for {len(properties_with_geometry)} properties with geometry")
        
        # Calculate map center from all properties
        all_centers = []
        for prop in properties_with_geometry:
            geometry = prop.get('GEOMETRY')
            if geometry:
                polygons = parse_wkt_multipolygon(str(geometry))
                for polygon in polygons:
                    center = get_polygon_center(polygon)
                    if center:
                        all_centers.append(center)
        
        if not all_centers:
            # Default to Belo Horizonte center if no valid coordinates
            map_center = [-19.9167, -43.9345]
            zoom_start = 10
        else:
            # Calculate overall center
            center_lat = sum(center[0] for center in all_centers) / len(all_centers)
            center_lon = sum(center[1] for center in all_centers) / len(all_centers)
            map_center = [center_lat, center_lon]
            zoom_start = 12
        
        # Create map
        m = folium.Map(
            location=map_center,
            zoom_start=zoom_start,
            tiles='OpenStreetMap'
        )
        
        # Add marker cluster for better performance
        marker_cluster = MarkerCluster().add_to(m)
        
        # Color palette for different property types
        colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 
                 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 
                 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']
        
        # Add properties to map
        for i, prop in enumerate(properties_with_geometry):
            geometry = prop.get('GEOMETRY')
            
            # Property info for popup
            endereco = prop.get('ENDERECO', 'N/A')
            bairro = prop.get('BAIRRO', 'N/A')
            indice = prop.get('INDICE CADASTRAL', 'N/A')
            tipo = prop.get('TIPO CONSTRUTIVO', 'N/A')
            area_terreno = prop.get('AREA TERRENO', 'N/A')
            area_construcao = prop.get('AREA CONSTRUCAO', 'N/A')
            valor_net = prop.get('NET VALOR', 'N/A')
            
            # Format value
            if isinstance(valor_net, (int, float)) and pd.notna(valor_net):
                valor_formatado = f"R$ {valor_net:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            else:
                valor_formatado = "N/A"
            
            popup_html = f"""
            <div style="width: 250px;">
                <h4>{endereco}</h4>
                <p><strong>Bairro:</strong> {bairro}</p>
                <p><strong>Índice:</strong> {indice}</p>
                <p><strong>Tipo:</strong> {tipo}</p>
                <p><strong>Área Terreno:</strong> {area_terreno} m²</p>
                <p><strong>Área Construção:</strong> {area_construcao} m²</p>
                <p><strong>Valor NET:</strong> {valor_formatado}</p>
            </div>
            """
            
            # Parse and add geometry
            polygons = parse_wkt_multipolygon(str(geometry))
            color = colors[i % len(colors)]
            
            for polygon in polygons:
                if polygon:
                    # Add polygon to map
                    folium.Polygon(
                        locations=polygon,
                        color=color,
                        weight=2,
                        fillColor=color,
                        fillOpacity=0.3,
                        popup=folium.Popup(popup_html, max_width=300)
                    ).add_to(m)
                    
                    # Add marker at center
                    center = get_polygon_center(polygon)
                    if center:
                        folium.Marker(
                            location=center,
                            popup=folium.Popup(popup_html, max_width=300),
                            icon=folium.Icon(color=color, icon='home', prefix='fa'),
                            tooltip=f"{endereco}, {bairro}"
                        ).add_to(marker_cluster)
        
        # Add a legend
        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; right: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
            <h4>Propriedades</h4>
            <p><i class="fa fa-home" style="color:red"></i> {len(properties_with_geometry)} propriedades encontradas</p>
            <p>🏠 Polígonos mostram área exata</p>
            <p>📍 Marcadores no centro de cada propriedade</p>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Return map as HTML
        return m._repr_html_()
        
    except ImportError:
        return "<p>❌ Bibliotecas de mapeamento não disponíveis. Execute: pip install folium streamlit-folium</p>"
    except Exception as e:
        print(f"Error creating map: {e}")
        return f"<p>❌ Erro ao criar mapa: {str(e)}</p>"

def render_property_map_streamlit(properties: List[Dict[str, Any]], map_style: str = "OpenStreetMap"):
    """
    Render property map in Streamlit using streamlit-folium.
    This is the recommended way for Streamlit apps.
    """
    try:
        import folium
        import streamlit_folium as st_folium
        from folium.plugins import MarkerCluster
        
        if not properties:
            st.warning("Nenhuma propriedade encontrada.")
            return
        
        # Filter properties with valid geometry
        properties_with_geometry = []
        for prop in properties:
            geometry = prop.get('GEOMETRY')
            if geometry and pd.notna(geometry) and str(geometry).strip():
                properties_with_geometry.append(prop)
        
        if not properties_with_geometry:
            st.warning("Nenhuma propriedade com dados geográficos encontrada.")
            return
        
        st.success(f"🗺️ Mostrando {len(properties_with_geometry)} propriedades no mapa")
        
        # Calculate map center from all properties
        all_centers = []
        for prop in properties_with_geometry:
            geometry = prop.get('GEOMETRY')
            if geometry:
                polygons = parse_wkt_multipolygon(str(geometry))
                for polygon in polygons:
                    center = get_polygon_center(polygon)
                    if center:
                        all_centers.append(center)
        
        if not all_centers:
            # Default to Belo Horizonte center if no valid coordinates
            map_center = [-19.9167, -43.9345]
            zoom_start = 10
        else:
            # Calculate overall center
            center_lat = sum(center[0] for center in all_centers) / len(all_centers)
            center_lon = sum(center[1] for center in all_centers) / len(all_centers)
            map_center = [center_lat, center_lon]
            zoom_start = 12
        
        # Map style configurations
        map_styles = {
            "OpenStreetMap": {
                "tiles": "OpenStreetMap", 
                "attr": None
            },
            "Satellite": {
                "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", 
                "attr": "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community"
            },
            "Terrain": {
                "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}", 
                "attr": "Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ, TomTom, Intermap, iPC, USGS, FAO, NPS, NRCAN, GeoBase, Kadaster NL, Ordnance Survey, Esri Japan, METI, Esri China (Hong Kong), and the GIS User Community"
            },
            "Dark": {
                "tiles": "CartoDB dark_matter", 
                "attr": None
            },
            "Light": {
                "tiles": "CartoDB positron", 
                "attr": None
            },
            "Streets": {
                "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}", 
                "attr": "Tiles &copy; Esri &mdash; Source: Esri, DeLorme, NAVTEQ, USGS, Intermap, iPC, NRCAN, Esri Japan, METI, Esri China (Hong Kong), swisstopo, and the GIS User Community"
            },
            "Physical": {
                "tiles": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Physical_Map/MapServer/tile/{z}/{y}/{x}", 
                "attr": "Tiles &copy; Esri &mdash; Source: US National Park Service"
            }
        }
        
        # Get style configuration
        style_config = map_styles.get(map_style, map_styles["OpenStreetMap"])
        
        # Create map with selected style
        m = folium.Map(
            location=map_center,
            zoom_start=zoom_start,
            tiles=style_config["tiles"],
            attr=style_config["attr"]
        )
        
        # Add marker cluster for better performance
        marker_cluster = MarkerCluster().add_to(m)
        
        # Color palette for different property types
        colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 
                 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 
                 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']
        
        # Add properties to map
        for i, prop in enumerate(properties_with_geometry):
            geometry = prop.get('GEOMETRY')
            
            # Property info for popup
            endereco = prop.get('ENDERECO', 'N/A')
            bairro = prop.get('BAIRRO', 'N/A')
            indice = prop.get('INDICE CADASTRAL', 'N/A')
            tipo = prop.get('TIPO CONSTRUTIVO', 'N/A')
            area_terreno = prop.get('AREA TERRENO', 'N/A')
            area_construcao = prop.get('AREA CONSTRUCAO', 'N/A')
            valor_net = prop.get('NET VALOR', 'N/A')
            
            # Format value
            if isinstance(valor_net, (int, float)) and pd.notna(valor_net):
                valor_formatado = f"R$ {valor_net:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            else:
                valor_formatado = "N/A"
            
            popup_html = f"""
            <div style="width: 250px;">
                <h4>{endereco}</h4>
                <p><strong>Bairro:</strong> {bairro}</p>
                <p><strong>Índice:</strong> {indice}</p>
                <p><strong>Tipo:</strong> {tipo}</p>
                <p><strong>Área Terreno:</strong> {area_terreno} m²</p>
                <p><strong>Área Construção:</strong> {area_construcao} m²</p>
                <p><strong>Valor NET:</strong> {valor_formatado}</p>
            </div>
            """
            
            # Parse and add geometry
            polygons = parse_wkt_multipolygon(str(geometry))
            color = colors[i % len(colors)]
            
            for polygon in polygons:
                if polygon:
                    # Add polygon to map
                    folium.Polygon(
                        locations=polygon,
                        color=color,
                        weight=2,
                        fillColor=color,
                        fillOpacity=0.3,
                        popup=folium.Popup(popup_html, max_width=300)
                    ).add_to(m)
                    
                    # Add marker at center
                    center = get_polygon_center(polygon)
                    if center:
                        folium.Marker(
                            location=center,
                            popup=folium.Popup(popup_html, max_width=300),
                            icon=folium.Icon(color=color, icon='home', prefix='fa'),
                            tooltip=f"{endereco}, {bairro}"
                        ).add_to(marker_cluster)
        
        # Render map in Streamlit
        st_folium.st_folium(m, width=700, height=400)
        
    except ImportError:
        st.error("❌ Bibliotecas de mapeamento não disponíveis. Execute: pip install folium streamlit-folium")
    except Exception as e:
        st.error(f"❌ Erro ao criar mapa: {str(e)}")

def get_property_map_summary(properties: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get summary statistics for properties that will be shown on the map."""
    if not properties:
        return {}
    
    # Filter properties with valid geometry
    properties_with_geometry = []
    for prop in properties:
        geometry = prop.get('GEOMETRY')
        if geometry and pd.notna(geometry) and str(geometry).strip():
            properties_with_geometry.append(prop)
    
    total_properties = len(properties)
    mappable_properties = len(properties_with_geometry)
    
    # Calculate total areas and values
    total_area_terreno = 0
    total_area_construcao = 0
    total_valor = 0
    property_types = {}
    neighborhoods = {}
    
    for prop in properties_with_geometry:
        # Areas
        area_terreno = prop.get('AREA TERRENO')
        if isinstance(area_terreno, (int, float)) and pd.notna(area_terreno):
            total_area_terreno += area_terreno
            
        area_construcao = prop.get('AREA CONSTRUCAO')
        if isinstance(area_construcao, (int, float)) and pd.notna(area_construcao):
            total_area_construcao += area_construcao
        
        # Value
        valor = prop.get('NET VALOR')
        if isinstance(valor, (int, float)) and pd.notna(valor):
            total_valor += valor
        
        # Property types
        tipo = prop.get('TIPO CONSTRUTIVO', 'N/A')
        property_types[tipo] = property_types.get(tipo, 0) + 1
        
        # Neighborhoods
        bairro = prop.get('BAIRRO', 'N/A')
        neighborhoods[bairro] = neighborhoods.get(bairro, 0) + 1
    
    return {
        'total_properties': total_properties,
        'mappable_properties': mappable_properties,
        'total_area_terreno': total_area_terreno,
        'total_area_construcao': total_area_construcao,
        'total_valor': total_valor,
        'property_types': property_types,
        'neighborhoods': neighborhoods
    }