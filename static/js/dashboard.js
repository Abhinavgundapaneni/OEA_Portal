function populate_asset_table(table, type) {
    table.children().remove()
    assets = get_json_data(type)
    for(var asset in assets) {
        tr = table.insertRow(0);
        var asset = tr.insertCell(-1);
        asset.innerHTML = asset['name']
        var version = tr.insertCell(-1);
        version.innerHTML = asset['version']
        var lastUpdatedTime = tr.insertCell(-1);
        lastUpdatedTime.innerHTML = asset['lastUpdatedTime']
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
    var displayTable = document.getElementById("installedAssetsTableBody")
    modulesBtn.addEventListener("click", e => {
        populate_asset_table(displayTable, "modules")
    })
})