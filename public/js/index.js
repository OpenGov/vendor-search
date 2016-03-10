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
