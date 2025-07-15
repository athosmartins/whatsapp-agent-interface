"""
Property Map Visualization Component

This module creates interactive maps showing property locations and geometries
for owners based on their mega_data_set information.
"""

import re
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st


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
        "Streets": "🛣️ Ruas",
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
        if wkt_string.startswith("MULTIPOLYGON"):
            coords_str = wkt_string[12:].strip()  # Remove 'MULTIPOLYGON'
        else:
            coords_str = wkt_string

        # Remove outer parentheses
        if coords_str.startswith("(") and coords_str.endswith(")"):
            coords_str = coords_str[1:-1].strip()

        polygons = []

        # Find all polygon groups: (((...)))
        polygon_pattern = r"\(\(\(([^)]+(?:\([^)]*\)[^)]*)*)\)\)\)"
        polygon_matches = re.findall(polygon_pattern, coords_str)

        if not polygon_matches:
            # Try simpler pattern for single polygon
            polygon_pattern = r"\(\(([^)]+)\)\)"
            polygon_matches = re.findall(polygon_pattern, coords_str)

        for match in polygon_matches:
            # Split coordinate pairs
            coord_pairs = match.split(",")
            polygon_coords = []

            for pair in coord_pairs:
                # Extract lat, lon from each pair
                coords = pair.strip().split()
                if len(coords) >= 2:
                    try:
                        lon = float(coords[0])
                        lat = float(coords[1])
                        # Folium expects [lat, lon]
                        polygon_coords.append([lat, lon])
                    except ValueError:
                        continue

            if polygon_coords:
                polygons.append(polygon_coords)

        # If still no matches, try direct coordinate extraction
        if not polygons:
            # Extract all coordinate pairs from the string
            coord_pattern = r"(-?\d+\.?\d*)\s+(-?\d+\.?\d*)"
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
    except Exception:
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
            geometry = prop.get("GEOMETRY")
            if geometry and pd.notna(geometry) and str(geometry).strip():
                properties_with_geometry.append(prop)

        if not properties_with_geometry:
            return "<p>Nenhuma propriedade com dados geográficos " "encontrada.</p>"

        print(
            f"Creating map for {len(properties_with_geometry)} "
            f"properties with geometry"
        )

        # Calculate map center from all properties
        all_centers = []
        for prop in properties_with_geometry:
            geometry = prop.get("GEOMETRY")
            if geometry:
                polygons = parse_wkt_multipolygon(str(geometry))
                for polygon in polygons:
                    center = get_polygon_center(polygon)
                    if center:
                        all_centers.append(center)

        if not all_centers:
            # Default to Belo Horizonte center if no valid coordinates
            map_center = [-19.9167, -43.9345]
            zoom_start = 16
        else:
            # Calculate overall center
            center_lat = sum(center[0] for center in all_centers) / len(all_centers)
            center_lon = sum(center[1] for center in all_centers) / len(all_centers)
            map_center = [center_lat, center_lon]
            zoom_start = 16

        # Create map
        m = folium.Map(
            location=map_center, zoom_start=zoom_start, tiles="OpenStreetMap"
        )

        # Add marker cluster for better performance
        marker_cluster = MarkerCluster().add_to(m)

        # Color palette for different property types
        colors = [
            "red",
            "blue",
            "green",
            "purple",
            "orange",
            "darkred",
            "lightred",
            "beige",
            "darkblue",
            "darkgreen",
            "cadetblue",
            "darkpurple",
            "white",
            "pink",
            "lightblue",
            "lightgreen",
            "gray",
            "black",
            "lightgray",
        ]

        # Add properties to map
        for i, prop in enumerate(properties_with_geometry):
            geometry = prop.get("GEOMETRY")

            # Property info for popup
            endereco = prop.get("ENDERECO", "N/A")
            bairro = prop.get("BAIRRO", "N/A")
            indice = prop.get("INDICE CADASTRAL", "N/A")
            tipo = prop.get("TIPO CONSTRUTIVO", "N/A")
            area_terreno = prop.get("AREA TERRENO", "N/A")
            area_construcao = prop.get("AREA CONSTRUCAO", "N/A")
            valor_net = prop.get("NET VALOR", "N/A")

            # Format value
            if isinstance(valor_net, (int, float)) and pd.notna(valor_net):
                valor_formatado = (
                    f"R$ {valor_net:,.2f}".replace(",", "X")
                    .replace(".", ",")
                    .replace("X", ".")
                )
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
                        popup=folium.Popup(popup_html, max_width=300),
                    ).add_to(m)

                    # Add marker at center
                    center = get_polygon_center(polygon)
                    if center:
                        folium.Marker(
                            location=center,
                            popup=folium.Popup(popup_html, max_width=300),
                            icon=folium.Icon(color=color, icon="home", prefix="fa"),
                            tooltip=f"{endereco}, {bairro}",
                        ).add_to(marker_cluster)

        # Add a legend
        legend_html = f"""
        <div style="position: fixed;
                    bottom: 50px; right: 50px; width: 200px; height: auto;
                    background-color: white; border:2px solid grey; z-index:9999;
                    font-size:14px; padding: 10px">
            <h4>Propriedades</h4>
            <p><i class="fa fa-home" style="color:red"></i>
            {len(properties_with_geometry)} propriedades encontradas</p>
            <p>🏠 Polígonos mostram área exata</p>
            <p>📍 Marcadores no centro de cada propriedade</p>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))

        # Return map as HTML
        return m._repr_html_()

    except ImportError:
        return (
            "<p>❌ Bibliotecas de mapeamento não disponíveis. Execute: "
            "pip install folium streamlit-folium</p>"
        )
    except Exception as e:
        print(f"Error creating map: {e}")
        return f"<p>❌ Erro ao criar mapa: {str(e)}</p>"


def render_property_map_streamlit(
    properties: List[Dict[str, Any]],
    map_style: str = "Light",
    enable_style_selector: bool = True,
    enable_extra_options: bool = True,
):
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
            geometry = prop.get("GEOMETRY")
            if geometry and pd.notna(geometry) and str(geometry).strip():
                properties_with_geometry.append(prop)

        if not properties_with_geometry:
            st.warning("Nenhuma propriedade com dados geográficos encontrada.")
            return

        # Removed properties count message as requested

        # Removed column info section - moved to advanced options

        # Calculate map center from all properties
        all_centers = []
        for prop in properties_with_geometry:
            geometry = prop.get("GEOMETRY")
            if geometry:
                polygons = parse_wkt_multipolygon(str(geometry))
                for polygon in polygons:
                    center = get_polygon_center(polygon)
                    if center:
                        all_centers.append(center)

        if not all_centers:
            # Default to Belo Horizonte center if no valid coordinates
            map_center = [-19.9167, -43.9345]
            zoom_start = 16
        else:
            # Calculate overall center
            center_lat = sum(center[0] for center in all_centers) / len(all_centers)
            center_lon = sum(center[1] for center in all_centers) / len(all_centers)
            map_center = [center_lat, center_lon]
            zoom_start = 16

        # Map style configurations
        map_styles = {
            "OpenStreetMap": {"tiles": "OpenStreetMap", "attr": None},
            "Satellite": {
                "tiles": (
                    "https://server.arcgisonline.com/ArcGIS/rest/services/"
                    "World_Imagery/MapServer/tile/{z}/{y}/{x}"
                ),
                "attr": (
                    "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, "
                    "USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, "
                    "UPR-EGP, and the GIS User Community"
                ),
            },
            "Terrain": {
                "tiles": (
                    "https://server.arcgisonline.com/ArcGIS/rest/services/"
                    "World_Topo_Map/MapServer/tile/{z}/{y}/{x}"
                ),
                "attr": (
                    "Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ, "
                    "TomTom, Intermap, iPC, USGS, FAO, NPS, NRCAN, "
                    "GeoBase, Kadaster NL, Ordnance Survey, Esri Japan, "
                    "METI, Esri China (Hong Kong), and the GIS User Community"
                ),
            },
            "Dark": {"tiles": "CartoDB dark_matter", "attr": None},
            "Light": {"tiles": "CartoDB positron", "attr": None},
            "Streets": {
                "tiles": (
                    "https://server.arcgisonline.com/ArcGIS/rest/services/"
                    "World_Street_Map/MapServer/tile/{z}/{y}/{x}"
                ),
                "attr": (
                    "Tiles &copy; Esri &mdash; Source: Esri, DeLorme, "
                    "NAVTEQ, USGS, Intermap, iPC, NRCAN, Esri Japan, "
                    "METI, Esri China (Hong Kong), swisstopo, and the "
                    "GIS User Community"
                ),
            },
            "Physical": {
                "tiles": (
                    "https://server.arcgisonline.com/ArcGIS/rest/services/"
                    "World_Physical_Map/MapServer/tile/{z}/{y}/{x}"
                ),
                "attr": "Tiles &copy; Esri &mdash; Source: US National Park Service",
            },
        }

        # Get style configuration - check advanced options first
        if enable_extra_options:
            selected_map_style = st.session_state.get("advanced_map_style", map_style)
        else:
            selected_map_style = map_style
        style_config = map_styles.get(selected_map_style, map_styles["Light"])

        # Store advanced options setup for later rendering
        advanced_options_enabled = enable_extra_options

        # Initialize default values when advanced options are disabled
        if not enable_extra_options:
            show_markers = False
            show_polygons = True
            marker_cluster_enabled = False
            polygon_opacity = 1.0
            polygon_weight = 1
            show_legend = True
            show_tooltips = True
            # Note: show_property_info not used in current implementation
            color_by_column = "Índice Sequencial"
            column_name_mapping = {}
            selected_tooltip_fields = [
                "BAIRRO",
                "ENDERECO",
                "AREA TERRENO",
                "TIPO CONSTRUTIVO",
                "AREA CONSTRUCAO",
                "FRACAO IDEAL",
            ]
            selected_popup_fields = [
                "ZONA FISCAL",
                "QUARTEIRAO",
                "LOTE",
                "INDICE CADASTRAL",
                "ZONEAMENTO",
                "ADE",
                "DESCRICAO ALTIMETRIA",
                "GRAU TOMBAMENTO",
            ]
        else:
            # Read values from advanced options session state, or use defaults
            show_markers = st.session_state.get("advanced_show_markers", False)
            show_polygons = st.session_state.get("advanced_show_polygons", True)
            marker_cluster_enabled = st.session_state.get(
                "advanced_marker_cluster", False
            )
            polygon_opacity = st.session_state.get("advanced_polygon_opacity", 1.0)
            polygon_weight = st.session_state.get("advanced_polygon_weight", 1)
            show_legend = st.session_state.get("advanced_show_legend", True)
            show_tooltips = st.session_state.get("advanced_show_tooltips", True)
            # Note: show_property_info not used in current implementation
            # Check if STATUS CONTATO column exists, otherwise default to "Índice Sequencial"
            default_color_column = "Índice Sequencial"
            if properties_with_geometry:
                # Check if STATUS CONTATO exists in the properties
                sample_prop = properties_with_geometry[0]
                if "STATUS CONTATO" in sample_prop:
                    default_color_column = "STATUS CONTATO"
            
            color_by_column = st.session_state.get(
                "advanced_color_by_column", default_color_column
            )
            column_name_mapping = {}
            selected_tooltip_fields = st.session_state.get(
                "advanced_tooltip_fields",
                [
                    "BAIRRO",
                    "ENDERECO",
                    "AREA TERRENO",
                    "TIPO CONSTRUTIVO",
                    "AREA CONSTRUCAO",
                    "FRACAO IDEAL",
                ],
            )
            selected_popup_fields = st.session_state.get(
                "advanced_popup_fields",
                [
                    "ZONA FISCAL",
                    "QUARTEIRAO",
                    "LOTE",
                    "INDICE CADASTRAL",
                    "ZONEAMENTO",
                    "ADE",
                    "DESCRICAO ALTIMETRIA",
                    "GRAU TOMBAMENTO",
                ],
            )

        # Re-create map with updated options
        m = folium.Map(
            location=map_center,
            zoom_start=zoom_start,
            tiles=style_config["tiles"],
            attr=style_config["attr"],
        )

        # Add marker cluster conditionally
        if marker_cluster_enabled and show_markers:
            marker_cluster = MarkerCluster().add_to(m)
        else:
            marker_cluster = m  # Add directly to map

        # Color palette for different values
        colors = [
            "red",
            "blue",
            "green",
            "purple",
            "orange",
            "darkred",
            "lightred",
            "beige",
            "darkblue",
            "darkgreen",
            "cadetblue",
            "darkpurple",
            "white",
            "pink",
            "lightblue",
            "lightgreen",
            "gray",
            "black",
            "lightgray",
        ]

        # Build color mapping based on selected column
        color_mapping = {}
        unique_values = []

        if color_by_column == "Índice Sequencial":
            # Use sequential index for colors
            for i, prop in enumerate(properties_with_geometry):
                color_mapping[i] = colors[i % len(colors)]
        else:
            # Use column values for colors - handle different data types
            # Get actual column name (handle conversation column mapping)
            actual_column = column_name_mapping.get(color_by_column, color_by_column)

            raw_values = []
            for prop in properties_with_geometry:
                raw_value = prop.get(actual_column, "N/A")

                # Handle different data types
                if pd.isna(raw_value) or raw_value is None:
                    clean_value = "N/A"
                elif isinstance(raw_value, (int, float)):
                    clean_value = str(raw_value)
                else:
                    clean_value = str(raw_value).strip()
                    if not clean_value:  # Handle empty strings
                        clean_value = "N/A"

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
            geometry = prop.get("GEOMETRY")

            # Property info for popup
            endereco = prop.get("ENDERECO", "N/A")
            bairro = prop.get("BAIRRO", "N/A")

            # Debug tooltip data for first property
            if i == 0:
                print("🔍 Debug first property tooltip data:")
                print(f"   - endereco: '{endereco}' (type: {type(endereco)})")
                print(f"   - bairro: '{bairro}' (type: {type(bairro)})")
                print(f"   - show_tooltips: {show_tooltips}")

            # Check if this is from conversations view
            conversation_id = prop.get("_conversation_id")

            # Create dynamic popup content based on selected fields
            popup_parts = []

            # Field mapping for conversation fields
            popup_field_mapping = {
                "👤 Nome do Contato": "_conversation_display_name",
                "📞 Telefone do Contato": "_conversation_phone",
                "🆔 ID da Conversa": "_conversation_id",
            }

            # Build popup content dynamically
            for field in selected_popup_fields:
                # Get actual field name (handle conversation field mapping)
                actual_field = popup_field_mapping.get(field, field)
                value = prop.get(actual_field)

                if value and str(value).strip() and str(value).strip() != "N/A":
                    # Format the field name for display
                    if (
                        field.startswith("👤")
                        or field.startswith("📞")
                        or field.startswith("🆔")
                    ):
                        display_name = field  # Keep emoji and name
                        icon = ""  # No extra icon needed
                    else:
                        # Add appropriate icons for property fields
                        field_icons = {
                            "ENDERECO": "🏠",
                            "BAIRRO": "📍",
                            "TIPO CONSTRUTIVO": "🏗️",
                            "AREA TERRENO": "📐",
                            "AREA CONSTRUCAO": "🏢",
                            "NET VALOR": "💰",
                            "INDICE CADASTRAL": "🔢",
                        }
                        icon = field_icons.get(field, "📋")
                        display_name = f"{icon} {field.replace('_', ' ').title()}"

                    # Format value based on field type
                    if (
                        field == "NET VALOR"
                        and isinstance(value, (int, float))
                        and pd.notna(value)
                    ):
                        formatted_value = (
                            f"R$ {value:,.2f}".replace(",", "X")
                            .replace(".", ",")
                            .replace("X", ".")
                        )
                    elif (
                        field in ["AREA TERRENO", "AREA CONSTRUCAO"]
                        and str(value).replace(".", "").isdigit()
                    ):
                        formatted_value = f"{value} m²"
                    else:
                        formatted_value = str(value).strip()

                    popup_parts.append(
                        f"<p><strong>{display_name}:</strong> {formatted_value}</p>"
                    )

            # Create popup HTML
            if popup_parts:
                popup_html = f"""
                <div style="width: 320px;">
                    {''.join(popup_parts)}
                </div>
                """
            else:
                # Fallback popup
                popup_html = f"""
                <div style="width: 300px;">
                    <h4>🏠 {endereco}</h4>
                    <p><strong>📍 Bairro:</strong> {bairro}</p>
                </div>
                """

            # Parse and add geometry
            polygons = parse_wkt_multipolygon(str(geometry))

            # Get color based on selected column
            if color_by_column == "Índice Sequencial":
                color = color_mapping[i]
            else:
                # Get actual column name (handle conversation column mapping)
                actual_column = column_name_mapping.get(
                    color_by_column, color_by_column
                )
                raw_value = prop.get(actual_column, "N/A")

                # Clean the value (same logic as in color mapping)
                if pd.isna(raw_value) or raw_value is None:
                    column_value = "N/A"
                elif isinstance(raw_value, (int, float)):
                    column_value = str(raw_value)
                else:
                    column_value = str(raw_value).strip()
                    if not column_value:
                        column_value = "N/A"

                color = color_mapping.get(column_value, "gray")

            for polygon in polygons:
                if polygon:
                    # Add polygon to map if enabled
                    if show_polygons:
                        # Create dynamic tooltip text based on selected fields
                        tooltip_parts = []

                        # Mapping for conversation fields
                        field_mapping = {
                            "👤 Nome do Contato": "_conversation_display_name",
                            "📞 Telefone do Contato": "_conversation_phone",
                            "🆔 ID da Conversa": "_conversation_id",
                        }

                        for field in selected_tooltip_fields:
                            # Get actual field name (handle conversation field mapping)
                            actual_field = field_mapping.get(field, field)
                            value = prop.get(actual_field)

                            if (
                                value
                                and str(value).strip()
                                and str(value).strip() != "N/A"
                            ):
                                # Format the field name for display
                                if (
                                    field.startswith("👤")
                                    or field.startswith("📞")
                                    or field.startswith("🆔")
                                ):
                                    display_name = (
                                        field.split(" ", 1)[1]
                                        if " " in field
                                        else field
                                    )  # Remove emoji
                                else:
                                    display_name = field.replace("_", " ").title()

                                tooltip_parts.append(
                                    f"{display_name}: {str(value).strip()}"
                                )

                        # Join with HTML line breaks for better readability
                        tooltip_text = (
                            "<br>".join(tooltip_parts)
                            if tooltip_parts
                            else "Informações não disponíveis"
                        )

                        # Create polygon with enhanced popup for processor navigation
                        conversation_id = prop.get("_conversation_id")
                        if conversation_id and conversation_id != "N/A":
                            # Simple hyperlink approach
                            processor_url = (
                                f"/Processor?conversation_id={conversation_id}"
                            )
                            enhanced_popup = (
                                popup_html
                                + f"""
                            <hr style="margin: 10px 0;">
                            <div style="text-align: center;">
                                <p style="font-size: 13px; margin: 8px 0;">
                                    <a href="{processor_url}" 
                                       target="_blank" 
                                       rel="noopener noreferrer"
                                       style="display: inline-block; background: #0066cc;
                                              color: white; text-decoration: none;
                                              padding: 8px 16px; border-radius: 4px;
                                              font-weight: bold; font-size: 13px;">
                                        📝 Abrir Conversa
                                    </a>
                                </p>
                                <p style="font-size: 11px; color: #666; margin: 5px 0;">
                                    ID: <code style="background: #f0f0f0; padding: 2px 4px;
                                               border-radius: 3px;">{conversation_id}</code>
                                </p>
                            </div>
                            """
                            )

                            polygon_obj = folium.Polygon(
                                locations=polygon,
                                color=color,
                                weight=polygon_weight,
                                fillColor=color,
                                fillOpacity=polygon_opacity,
                                popup=folium.Popup(enhanced_popup, max_width=420),
                            )
                        else:
                            polygon_obj = folium.Polygon(
                                locations=polygon,
                                color=color,
                                weight=polygon_weight,
                                fillColor=color,
                                fillOpacity=polygon_opacity,
                                popup=folium.Popup(popup_html, max_width=320),
                            )

                        # Add tooltip to polygon if enabled and valid
                        if (
                            show_tooltips
                            and tooltip_text
                            and len(tooltip_text.strip()) > 3
                        ):
                            polygon_obj.add_child(
                                folium.Tooltip(tooltip_text, permanent=False)
                            )

                        polygon_obj.add_to(m)

                    # Add marker at center if enabled
                    if show_markers:
                        center = get_polygon_center(polygon)
                        if center:
                            # Use the same enhanced popup for markers
                            marker_popup = (
                                enhanced_popup
                                if conversation_id and conversation_id != "N/A"
                                else popup_html
                            )
                            marker_popup_width = (
                                420
                                if conversation_id and conversation_id != "N/A"
                                else 300
                            )

                            # Create marker with proper tooltip
                            marker = folium.Marker(
                                location=center,
                                popup=folium.Popup(
                                    marker_popup, max_width=marker_popup_width
                                ),
                                icon=folium.Icon(color=color, icon="home", prefix="fa"),
                            )

                            # Add tooltip if enabled - reuse the same tooltip text from polygon
                            if (
                                show_tooltips
                                and tooltip_text
                                and len(tooltip_text.strip()) > 3
                            ):
                                marker.add_child(
                                    folium.Tooltip(tooltip_text, permanent=False)
                                )

                            marker.add_to(marker_cluster)

        # Add legend if enabled
        if show_legend:
            # Create color legend based on selected column
            color_legend = ""
            if color_by_column != "Índice Sequencial" and unique_values:
                color_legend = (
                    f"<p><strong>🎨 Cores por {color_by_column}:</strong></p>"
                )
                for value in unique_values[
                    :5
                ]:  # Show max 5 values to avoid overcrowding
                    color = color_mapping.get(value, "gray")
                    color_legend += (
                        f"<p><span style='color:{color}'>●</span> {value}</p>"
                    )
                if len(unique_values) > 5:
                    color_legend += f"<p>... e mais {len(unique_values)-5} valores</p>"

            legend_html = f"""
            <div style="position: fixed;
                        bottom: 50px; right: 50px; width: 240px; height: auto;
                        background-color: white; border:2px solid grey;
                        z-index:9999; font-size:12px; padding: 10px;
                        border-radius: 5px;">
                {color_legend}
            </div>
            """
            m.get_root().html.add_child(folium.Element(legend_html))

        # Render map in Streamlit with full width and reasonable height
        map_data = st_folium.st_folium(m, use_container_width=True, height=600)
        
        # Debug: Show current map state
        if st.session_state.get('debug_mode', False):
            if map_data and 'zoom' in map_data:
                st.write(f"🔍 Current Map Zoom Level: **{map_data['zoom']}**")
            if map_data and 'center' in map_data:
                center = map_data['center']
                st.write(f"📍 Current Map Center: **[{center['lat']:.6f}, {center['lng']:.6f}]**")
            if map_data and 'bounds' in map_data:
                bounds = map_data['bounds']
                st.write(f"📐 Current Map Bounds:")
                st.write(f"   - North: {bounds['_northEast']['lat']:.6f}")
                st.write(f"   - South: {bounds['_southWest']['lat']:.6f}")
                st.write(f"   - East: {bounds['_northEast']['lng']:.6f}")
                st.write(f"   - West: {bounds['_southWest']['lng']:.6f}")

        # Advanced options button below the map (with reduced spacing)
        if advanced_options_enabled:
            # Add custom CSS to reduce spacing
            st.markdown(
                """
                <style>
                .stButton > button {
                    margin-top: -20px !important;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            
            if st.button("🛠️ Opções Avançadas do Mapa"):
                st.session_state.show_advanced_options = not (
                    st.session_state.get("show_advanced_options", False)
                )

            if st.session_state.get("show_advanced_options", False):
                st.markdown("---")
                st.subheader("🛠️ Opções Avançadas do Mapa")

                # Map Style Selector
                map_styles = {
                    "Light": {"tiles": "openstreetmap", "attr": "OpenStreetMap"},
                    "Dark": {"tiles": "cartodbdark_matter", "attr": "CartoDB"},
                    "Satellite": {
                        "tiles": (
                            "https://server.arcgisonline.com/ArcGIS/rest/services/"
                            "World_Imagery/MapServer/tile/{z}/{y}/{x}"
                        ),
                        "attr": "Esri",
                    },
                    "Terrain": {
                        "tiles": (
                            "https://stamen-tiles.a.ssl.fastly.net/"
                            "terrain/{z}/{x}/{y}.png"
                        ),
                        "attr": "Stamen",
                    },
                    "Watercolor": {
                        "tiles": (
                            "https://stamen-tiles.a.ssl.fastly.net/"
                            "watercolor/{z}/{x}/{y}.png"
                        ),
                        "attr": "Stamen",
                    },
                }

                st.selectbox(
                    "🎨 Estilo do Mapa",
                    options=list(map_styles.keys()),
                    format_func=lambda x: f"{x} Map",
                    key="advanced_map_style",
                    help="Escolha o estilo de visualização do mapa",
                )

                # Visual Options
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.checkbox(
                        "📍 Mostrar Marcadores",
                        value=False,
                        key="advanced_show_markers",
                        help="Mostrar marcadores no centro das propriedades",
                    )
                    st.checkbox(
                        "🔺 Mostrar Polígonos",
                        value=True,
                        key="advanced_show_polygons",
                        help="Mostrar contornos das propriedades",
                    )
                    st.checkbox(
                        "🎯 Agrupar Marcadores",
                        value=False,
                        key="advanced_marker_cluster",
                        help="Agrupar marcadores próximos",
                    )

                with col2:
                    st.slider(
                        "🎨 Opacidade dos Polígonos",
                        0.0,
                        1.0,
                        1.0,
                        0.1,
                        key="advanced_polygon_opacity",
                        help="Transparência dos polígonos",
                    )
                    st.slider(
                        "📏 Espessura das Bordas",
                        1,
                        5,
                        1,
                        key="advanced_polygon_weight",
                        help="Espessura das linhas dos polígonos",
                    )
                    st.checkbox(
                        "📋 Mostrar Legenda",
                        value=True,
                        key="advanced_show_legend",
                        help="Mostrar legenda no mapa",
                    )

                with col3:
                    st.checkbox(
                        "🏷️ Mostrar Tooltips",
                        value=True,
                        key="advanced_show_tooltips",
                        help="Mostrar informações ao passar o mouse",
                    )
                    st.checkbox(
                        "ℹ️ Informações nos Popups",
                        value=True,
                        key="advanced_show_property_info",
                        help="Mostrar informações detalhadas nos popups",
                    )

                # Color-by-column selector
                all_columns = set()
                for prop in properties_with_geometry:
                    all_columns.update(prop.keys())

                excluded_columns = {
                    "GEOMETRY",
                    "geometry",
                    "id",
                    "ID",
                    "_id",
                    "_conversation_id",
                }
                available_columns = sorted(
                    [col for col in all_columns if col not in excluded_columns]
                )
                color_options = ["Índice Sequencial"] + available_columns
                
                # Set default index based on STATUS CONTATO availability
                default_index = 0
                if "STATUS CONTATO" in available_columns:
                    default_index = color_options.index("STATUS CONTATO")

                st.selectbox(
                    "🎨 Colorir por:",
                    options=color_options,
                    index=default_index,
                    key="advanced_color_by_column",
                    help="Escolha uma coluna para colorir as propriedades",
                )

                # Field Selection
                col1, col2 = st.columns(2)

                with col1:
                    default_tooltip_fields = [
                        "BAIRRO",
                        "ENDERECO",
                        "AREA TERRENO",
                        "TIPO CONSTRUTIVO",
                        "AREA CONSTRUCAO",
                        "FRACAO IDEAL",
                        'STATUS CONTATO',
                    ]
                    st.multiselect(
                        "🏷️ Campos no Tooltip:",
                        options=available_columns,
                        default=[
                            f for f in default_tooltip_fields if f in available_columns
                        ],
                        key="advanced_tooltip_fields",
                        help=("Escolha quais informações mostrar ao passar o mouse"),
                    )

                with col2:
                    default_popup_fields = [
                        "ZONA FISCAL",
                        "QUARTEIRAO",
                        "LOTE",
                        "INDICE CADASTRAL",
                        "ZONEAMENTO",
                        "ADE",
                        "DESCRICAO ALTIMETRIA",
                        "GRAU TOMBAMENTO",
                    ]
                    available_popup_options = [
                        col
                        for col in available_columns
                        if col
                        not in st.session_state.get("advanced_tooltip_fields", [])
                    ]
                    st.multiselect(
                        "📋 Campos no Popup:",
                        options=available_popup_options,
                        default=[
                            f
                            for f in default_popup_fields
                            if f in available_popup_options
                        ],
                        key="advanced_popup_fields",
                        help=(
                            "Escolha quais informações mostrar nos popups ao "
                            "clicar nas propriedades"
                        ),
                    )


                # Apply button to refresh map with new settings
                col1, col2 = st.columns([1, 3])
                with col1:
                    if st.button("🔄 Aplicar Configurações", type="primary"):
                        st.rerun()
                with col2:
                    st.info(
                        "💡 Clique em 'Aplicar Configurações' para atualizar "
                        "o mapa com suas alterações."
                    )

    except ImportError:
        st.error(
            "❌ Bibliotecas de mapeamento não disponíveis. Execute: "
            "pip install folium streamlit-folium"
        )
    except Exception as e:
        st.error(f"❌ Erro ao criar mapa: {str(e)}")


def get_property_map_summary(properties: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get summary statistics for properties that will be shown on the map."""
    if not properties:
        return {}

    # Filter properties with valid geometry
    properties_with_geometry = []
    for prop in properties:
        geometry = prop.get("GEOMETRY")
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
        area_terreno = prop.get("AREA TERRENO")
        if isinstance(area_terreno, (int, float)) and pd.notna(area_terreno):
            total_area_terreno += area_terreno

        area_construcao = prop.get("AREA CONSTRUCAO")
        if isinstance(area_construcao, (int, float)) and pd.notna(area_construcao):
            total_area_construcao += area_construcao

        # Value
        valor = prop.get("NET VALOR")
        if isinstance(valor, (int, float)) and pd.notna(valor):
            total_valor += valor

        # Property types
        tipo = prop.get("TIPO CONSTRUTIVO", "N/A")
        property_types[tipo] = property_types.get(tipo, 0) + 1

        # Neighborhoods
        bairro = prop.get("BAIRRO", "N/A")
        neighborhoods[bairro] = neighborhoods.get(bairro, 0) + 1

    return {
        "total_properties": total_properties,
        "mappable_properties": mappable_properties,
        "total_area_terreno": total_area_terreno,
        "total_area_construcao": total_area_construcao,
        "total_valor": total_valor,
        "property_types": property_types,
        "neighborhoods": neighborhoods,
    }
