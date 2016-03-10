var max = 80000;

var scale = d3.scale.linear()
   .domain([0, max])
   .range([0, 3000]);

var color_scale = chroma.scale("OrRd")

var scale2 = d3.scale.linear()
   .domain([0,max])
   .range([0, 1]);

console.log(color_scale(scale2(80000)).hex())

var layer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
  attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
});


var map = L.map('map').setView([39.9626487,-83.00349], 7);

var data = {
   "type": "FeatureCollection",
   "features": [
  {
    "type": "Feature",
    "geometry": {
       "type": "Point",
       "coordinates":  [ -83.00349,39.9626487 ]
    },
    "properties": {
    "entity":"Columbus",
    "total":1168.32,
    "population":809798,
    "count":12,
    "max":97.56,
    "75th":97.32,
    "25th":97.32
    }
  },
  {
    "type": "Feature",
    "geometry": {
       "type": "Point",
       "coordinates":  [ -80.694757,40.887518 ]
    },
    "properties": {
    "entity":"City of Columbiana, OH",
    "total":69375.79,
    "population":null,
    "count":354,
    "max":669.28,
    "75th":266.89,
    "25th":84.4175
    }
  },
  {
    "type": "Feature",
    "geometry": {
       "type": "Point",
       "coordinates":  [ -84.3605022,39.8631101 ]
    },
    "properties": {
    "entity":"City of Clayton",
    "total":37930.55,
    "population":null,
    "count":300,
    "max":729.99,
    "75th":168.0925,
    "25th":55.11
    }
  },
  {
    "type": "Feature",
    "geometry": {
       "type": "Point",
       "coordinates":  [ -81.4840186,41.1387343 ]
    },
    "properties": {
    "entity":"Cuyahoga Falls",
    "total":30662.83,
    "population":null,
    "count":161,
    "max":1214.2,
    "75th":173.4,
    "25th":25.395
    }
  },
  {
    "type": "Feature",
    "geometry": {
       "type": "Point",
       "coordinates":  [ -84.4091013,39.8348289 ]
    },
    "properties": {
    "entity":"City of Brookville (Montgomery)",
    "total":30795.41,
    "population":null,
    "count":403,
    "max":399.99,
    "75th":99.86,
    "25th":-0.2
    }
  },
  {
    "type": "Feature",
    "geometry": {
       "type": "Point",
       "coordinates":  [ -82.9391206,39.9574626 ]
    },
    "properties": {
    "entity":"Bexley",
    "total":24094.1,
    "population":13252,
    "count":22,
    "max":5003.82,
    "75th":1962.995,
    "25th":100.42
    }
  }
]
}

test = _.sortBy(data, "total");
console.log(test);

map.addLayer(layer);

var circle = {
    radius: 8,
    fillColor: "#74acb8",
    color: "#555",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.3
};

L.geoJson(data, {
   pointToLayer: function (feature, latlng) {   
      console.log(feature.properties)
      var row = $("<tr />")
      $("#table").find("tbody").append(row)
      
      var total = feature.properties.total;
      console.log("scale it: ", scale(total))
      
      var color = color_scale(scale2(total)).hex();
      
      circle.fillColor = color;
      
      var area = scale(total);
      var radius = Math.sqrt(area / Math.PI)
      circle.radius = radius;
      
      _.each(feature.properties, function(prop, key){
         console.log(key)
         
         var cell = $("<td class=" + key + ">" + prop + "</td>")

         if (key == "total") {
            prop = "$" + prop;
         }
         
         if (key == "entity") {
            cell.prepend("<div class='color_key' style='background-color: " +  color + "'></div>")
         }

         
         row.append(cell);
      })
      
      return L.circleMarker(latlng, circle);
   }
}).addTo(map);