export function clickMap(e) {
    if (window.mapMarker) {
        window.map.removeLayer(window.mapMarker);
        window.map.removeLayer(window.geojson);
    }
    window.mapMarker = new L.Marker(e.latlng, {draggable: false});
    window.map.addLayer(window.mapMarker)

    window.geojson = new L.geoJSON.ajax("./geojson/neighbourhoods.geojson", {
        style: window.geojsonStyles,
        filter: (feature, layer) => {
            // console.log(feature);
            let showOnMap = inside([e.latlng.lng, e.latlng.lat], feature.geometry.coordinates[0][0]);
            if (showOnMap) {
                window.neighbourhood_group = feature.properties.neighbourhood_group;
                window.neighbourhood = feature.properties.neighbourhood;
                document.getElementById('neighborhood_group').innerHTML = window.neighbourhood_group;
                document.getElementById('neighborhood').innerHTML = window.neighbourhood;
            }
            return feature.properties.show_on_map = showOnMap;
        }
    })
    window.geojson.on('data:loaded', () => {
        window.geojson.addTo(window.map)
    })

    toggleSidebar(true);
    setCoordinates(e.latlng.lat.toFixed(4), e.latlng.lng.toFixed(4));
}

export function toggleSidebar(open) {
    let translationValue = open ? '0px' : '340px';
    document.getElementById('sidebar').style.transform = `translateX(${translationValue})`;
    
    if ((window.mapMarker || window.geojson) && !open) {
        window.mapMarker && window.map.removeLayer(window.mapMarker);
        window.geojson && window.map.removeLayer(window.geojson);
        document.getElementById('sample3').value = "";
    }
}

export function setCoordinates(lat, lng) {
    window.lat = lat;
    window.lng = lng;
    document.getElementById('coordinates-latitude').innerHTML = `${Math.abs(lat)}&#176;${lat > 0 ? 'N' : 'S'}`;
    document.getElementById('coordinates-longtitude').innerHTML = `${Math.abs(lng)}&#176;${lng > 0 ? 'E' : 'W'}`;
}

function inside(point, vs) {
    // ray-casting algorithm based on
    // https://wrf.ecse.rpi.edu/Research/Short_Notes/pnpoly.html/pnpoly.html
    
    var x = point[0], y = point[1];
    
    var inside = false;
    for (var i = 0, j = vs.length - 1; i < vs.length; j = i++) {
        var xi = vs[i][0], yi = vs[i][1];
        var xj = vs[j][0], yj = vs[j][1];
        
        var intersect = ((yi > y) != (yj > y))
            && (x < (xj - xi) * (y - yi) / (yj - yi) + xi);
        if (intersect) inside = !inside;
    }
    
    return inside;
};