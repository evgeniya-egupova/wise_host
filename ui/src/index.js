import { clickMap, toggleSidebar } from './utils/map-utils.js';

(function() {


    window.map = L.map('map').setView([40.7049, -73.9507], 13);;

    L.tileLayer('https://api.maptiler.com/maps/streets/{z}/{x}/{y}.png?key=c6hrWvUSCd88Gfqy2L7Q', {
        attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
        maxZoom: 18,
        id: 'mapbox/streets-v11',
        tileSize: 512,
        zoomOffset: -1,
        accessToken: 'your.mapbox.access.token'
    }).addTo(map);

    window.geojsonStyles = {
        "color": "#7209b7",
        "weight": 2,
        "opacity": 0.65
    };

    window.map.on('click', clickMap);
    document.getElementById('close-icon').addEventListener('click', () => toggleSidebar(false));
    document.getElementById('get-price-button').addEventListener('click', () => onGetPriceClick());

})();

function onGetPriceClick() {

    let spinnerLoader = document.createElement('div');
    spinnerLoader.className = 'mdl-spinner mdl-spinner--single-color mdl-js-spinner is-active';

    let formData = {
        roomType: document.getElementById('sample2').value,
        minimumNights: document.getElementById('sample3').value,
        availability: document.getElementById('sample4').value,
        listingTitle: document.getElementById('sample5').value
    }

    let reqBody = {
        "latitude": Number(window.lat),
        "longitude": Number(window.lng),
        "minimum_nights": Number(formData.minimumNights),
        "availability": Number(formData.availability),
        "neighbourhood": window.neighbourhood,
        "neighbourhood_group": window.neighbourhood_group,
        "room_type": formData.roomType,
        "name": formData.listingTitle
    };

    // showMarkerPopup(Math.floor(Math.random() * Math.floor(150)))

    let xhr = new XMLHttpRequest()
    xhr.withCredentials = false;
    xhr.open('POST', 'http://35.204.88.10:5000', true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.send(JSON.stringify(reqBody));

    document.getElementById('p2').classList.remove('app-loader');

    xhr.onload = function() {
        if (xhr.status != 200) { // анализируем HTTP-статус ответа, если статус не 200, то произошла ошибка
            alert(`Ошибка ${xhr.status}: ${xhr.statusText}`); // Например, 404: Not Found
            document.getElementById('p2').classList.add('app-loader');
        } else { // если всё прошло гладко, выводим результат
            console.log(xhr.response);
            showMarkerPopup(Math.round(Math.abs(JSON.parse(xhr.response).data)));
            document.getElementById('p2').classList.add('app-loader');
        }
    };

    console.log(reqBody);
}


function showMarkerPopup(price) {
    if (window.mapMarker) {
        let popup = L.popup({className: 'leaflet-popup-content-wrapper' })
            .setContent(`<p>$${price}</p>`)
        window.mapMarker.bindPopup(popup).openPopup()
    }
}