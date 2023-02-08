function populate_asset_table(table, type) {
    while(table.rows.length > 0) {
        table.deleteRow(0);
      }
    assets = get_json_data(type)
    for(var asset in assets) {
        tr = table.insertRow(0);
        var asset_name = tr.insertCell(-1);
        asset_name.innerHTML = asset['Name']
        var version = tr.insertCell(-1);
        version.innerHTML = asset['Version']
        var lastUpdatedTime = tr.insertCell(-1);
        lastUpdatedTime.innerHTML = asset['LastUpdatedTime']
        var deleteBtn = document.createElement("button")
        deleteBtn.classList.add('btn')
        deleteBtn.classList.add('btn-primary')
        deleteBtn.innerHTML = 'delete';
        deleteBtnCell = tr.insertCell(-1);
        deleteBtnCell.appendChild(deleteBtn)
    }
}

function get_json_data(name) {
    var data_str = document.getElementById(name).textContent
    data_str = data_str.replace(/&quot;/ig,'"');
    data_str = data_str.replaceAll("'", "\"")
    var data = JSON.parse(data_str);
    return data
}

window.addEventListener("load", function(){
    var modulesBtn = document.getElementById("modulesBtn")
    var packagesBtn = document.getElementById("packagesBtn")
    var schemasBtn = document.getElementById("schemasBtn")
    var tbody = document.getElementById("installedAssetsTableBody")
    var table = document.getElementById("installedAssetsTable")
    table.style.display = "table"
    modulesBtn.addEventListener("click", e => {
        populate_asset_table(tbody, "modules")
    })
})