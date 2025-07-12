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
        "Light": "☀️ Claro",
        "OpenStreetMap": "🗺️ OpenStreetMap",
        "Satellite": "🛰️ Satélite",
        "Terrain": "🏔️ Terreno",
        "Streets": "🛣️ Ruas"
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

def render_property_map_streamlit(properties: List[Dict[str, Any]], map_style: str = "Light", enable_style_selector: bool = True, enable_extra_options: bool = True):
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
        
        # Show available columns info for debugging
        if enable_extra_options:
            with st.expander("🔍 Informações das Colunas", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Colunas Disponíveis:**")
                    all_cols = set()
                    for prop in properties_with_geometry:
                        all_cols.update(prop.keys())
                    for col in sorted(all_cols):
                        if col not in {'GEOMETRY', 'geometry', 'id', 'ID', '_id'}:
                            st.write(f"• {col}")
                
                with col2:
                    st.write("**Exemplo de Dados:**")
                    if properties_with_geometry:
                        sample_prop = properties_with_geometry[0]
                        for key, value in sample_prop.items():
                            if key not in {'GEOMETRY', 'geometry'}:
                                display_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                                st.write(f"**{key}**: {display_value}")
        
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
            # Calculate overall center and optimal zoom level
            center_lat = sum(center[0] for center in all_centers) / len(all_centers)
            center_lon = sum(center[1] for center in all_centers) / len(all_centers)
            map_center = [center_lat, center_lon]
            
            # Calculate bounding box to determine optimal zoom
            min_lat = min(center[0] for center in all_centers)
            max_lat = max(center[0] for center in all_centers)
            min_lon = min(center[1] for center in all_centers)
            max_lon = max(center[1] for center in all_centers)
            
            # Calculate distance spans
            lat_span = max_lat - min_lat
            lon_span = max_lon - min_lon
            max_span = max(lat_span, lon_span)
            
            # Calculate optimal zoom based on span (more properties = zoom out more)
            if max_span == 0:
                zoom_start = 16  # Single point
            elif max_span < 0.001:
                zoom_start = 15  # Very close properties
            elif max_span < 0.005:
                zoom_start = 14  # Close properties  
            elif max_span < 0.01:
                zoom_start = 13  # Nearby properties
            elif max_span < 0.05:
                zoom_start = 12  # Same neighborhood
            elif max_span < 0.1:
                zoom_start = 11  # Same area
            elif max_span < 0.2:
                zoom_start = 10  # Same city area
            else:
                zoom_start = 9   # Wide area
        
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
        style_config = map_styles.get(map_style, map_styles["Light"])
        
        # Add map controls and options if enabled
        if enable_extra_options:
            st.subheader("🛠️ Opções Avançadas do Mapa")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Marker options
                show_markers = st.checkbox("📍 Mostrar Marcadores", value=True, help="Mostrar marcadores no centro das propriedades")
                show_polygons = st.checkbox("🔺 Mostrar Polígonos", value=True, help="Mostrar contornos das propriedades") 
                marker_cluster_enabled = st.checkbox("🎯 Agrupar Marcadores", value=True, help="Agrupar marcadores próximos")
            
            with col2:
                # Visual options
                polygon_opacity = st.slider("🎨 Opacidade dos Polígonos", 0.0, 1.0, 1.0, 0.1, help="Transparência dos polígonos")
                polygon_weight = st.slider("📏 Espessura das Bordas", 1, 5, 1, help="Espessura das linhas dos polígonos")
                show_legend = st.checkbox("📋 Mostrar Legenda", value=True, help="Mostrar legenda no mapa")
                
                # Color-by-column selector - dynamically get all available columns
                all_columns = set()
                for prop in properties_with_geometry:
                    all_columns.update(prop.keys())
                
                # Filter out technical columns and sort alphabetically
                excluded_columns = {'GEOMETRY', 'geometry', 'id', 'ID', '_id'}
                available_columns = ['Índice Sequencial'] + sorted([
                    col for col in all_columns 
                    if col not in excluded_columns and col.strip()  # Exclude empty/whitespace columns
                ])
                
                color_by_column = st.selectbox(
                    "🎨 Colorir por:",
                    options=available_columns,
                    index=0,
                    help="Escolha qual coluna usar para definir as cores dos polígonos"
                )
                
                # Show unique values count for selected column
                if color_by_column != "Índice Sequencial":
                    unique_count = len(set(
                        str(prop.get(color_by_column, 'N/A')).strip() or 'N/A'
                        for prop in properties_with_geometry
                    ))
                    st.caption(f"📊 {unique_count} valores únicos em '{color_by_column}'")
            
            with col3:
                # Info options
                show_tooltips = st.checkbox("💬 Mostrar Tooltips", value=True, help="Mostrar informações ao passar o mouse")
                show_property_info = st.checkbox("ℹ️ Info Detalhada nos Popups", value=True, help="Mostrar informações completas nos popups")
                
        else:
            # Default values when options are disabled
            show_markers = True
            show_polygons = True
            marker_cluster_enabled = True
            polygon_opacity = 1.0  # Changed to 1.0 as requested
            polygon_weight = 1     # Changed to 1 as requested
            show_legend = True
            show_tooltips = True
            show_property_info = True
            color_by_column = "Índice Sequencial"
        
        # Re-create map with updated options
        m = folium.Map(
            location=map_center,
            zoom_start=zoom_start,
            tiles=style_config["tiles"],
            attr=style_config["attr"]
        )
        
        # Add marker cluster conditionally
        if marker_cluster_enabled and show_markers:
            marker_cluster = MarkerCluster().add_to(m)
        else:
            marker_cluster = m  # Add directly to map
        
        # Color palette for different values
        colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred', 
                 'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple', 'white', 
                 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 'lightgray']
        
        # Build color mapping based on selected column
        color_mapping = {}
        unique_values = []
        
        if color_by_column == "Índice Sequencial":
            # Use sequential index for colors
            for i, prop in enumerate(properties_with_geometry):
                color_mapping[i] = colors[i % len(colors)]
        else:
            # Use column values for colors - handle different data types
            raw_values = []
            for prop in properties_with_geometry:
                raw_value = prop.get(color_by_column, 'N/A')
                
                # Handle different data types
                if pd.isna(raw_value) or raw_value is None:
                    clean_value = 'N/A'
                elif isinstance(raw_value, (int, float)):
                    clean_value = str(raw_value)
                else:
                    clean_value = str(raw_value).strip()
                    if not clean_value:  # Handle empty strings
                        clean_value = 'N/A'
                
                raw_values.append(clean_value)
                if clean_value not in unique_values:
                    unique_values.append(clean_value)
            
            # Sort unique values for consistent color assignment
            unique_values.sort()
            
            # Assign colors to unique values
            for i, value in enumerate(unique_values):
                color_mapping[value] = colors[i % len(colors)]
        
        # Add properties to map with updated options
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
            
            # Create popup content based on options
            if show_property_info:
                popup_html = f"""
                <div style="width: 280px;">
                    <h4>{endereco}</h4>
                    <p><strong>Bairro:</strong> {bairro}</p>
                    <p><strong>Índice:</strong> {indice}</p>
                    <p><strong>Tipo:</strong> {tipo}</p>
                    <p><strong>Área Terreno:</strong> {area_terreno} m²</p>
                    <p><strong>Área Construção:</strong> {area_construcao} m²</p>
                    <p><strong>Valor NET:</strong> {valor_formatado}</p>
                </div>
                """
            else:
                popup_html = f"""
                <div style="width: 200px;">
                    <h4>{endereco}</h4>
                    <p><strong>Bairro:</strong> {bairro}</p>
                </div>
                """
            
            # Parse and add geometry
            polygons = parse_wkt_multipolygon(str(geometry))
            
            # Get color based on selected column
            if color_by_column == "Índice Sequencial":
                color = color_mapping[i]
            else:
                column_value = str(prop.get(color_by_column, 'N/A'))
                color = color_mapping.get(column_value, 'gray')
            
            for polygon in polygons:
                if polygon:
                    # Add polygon to map if enabled
                    if show_polygons:
                        folium.Polygon(
                            locations=polygon,
                            color=color,
                            weight=polygon_weight,
                            fillColor=color,
                            fillOpacity=polygon_opacity,
                            popup=folium.Popup(popup_html, max_width=300)
                        ).add_to(m)
                    
                    # Add marker at center if enabled
                    if show_markers:
                        center = get_polygon_center(polygon)
                        if center:
                            # Create marker with proper tooltip
                            marker = folium.Marker(
                                location=center,
                                popup=folium.Popup(popup_html, max_width=300),
                                icon=folium.Icon(color=color, icon='home', prefix='fa')
                            )
                            
                            # Add tooltip if enabled
                            if show_tooltips:
                                marker.add_child(folium.Tooltip(f"{endereco}, {bairro}"))
                            
                            marker.add_to(marker_cluster)
        
        # Add legend if enabled
        if show_legend:
            # Create color legend based on selected column
            color_legend = ""
            if color_by_column != "Índice Sequencial" and unique_values:
                color_legend = f"<p><strong>🎨 Cores por {color_by_column}:</strong></p>"
                for value in unique_values[:5]:  # Show max 5 values to avoid overcrowding
                    color = color_mapping.get(value, 'gray')
                    color_legend += f"<p><span style='color:{color}'>●</span> {value}</p>"
                if len(unique_values) > 5:
                    color_legend += f"<p>... e mais {len(unique_values)-5} valores</p>"
            
            legend_html = f"""
            <div style="position: fixed; 
                        bottom: 50px; right: 50px; width: 240px; height: auto; 
                        background-color: white; border:2px solid grey; z-index:9999; 
                        font-size:12px; padding: 10px; border-radius: 5px;">
                <h4>🏠 Propriedades</h4>
                <p><i class="fa fa-home" style="color:red"></i> {len(properties_with_geometry)} propriedades encontradas</p>
                <p>🔺 Polígonos mostram área exata</p>
                <p>📍 Marcadores no centro de cada propriedade</p>
                <p>🎯 Zoom otimizado automaticamente</p>
                {color_legend}
            </div>
            """
            m.get_root().html.add_child(folium.Element(legend_html))
        
        # Render map in Streamlit with full width and increased height (50% more than 400 = 600)
        st_folium.st_folium(m, use_container_width=True, height=600)
        
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