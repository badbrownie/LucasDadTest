
var yOffset = -173;
var xOffset = -11;

function swap(array,a,b){
	var num = array[a];
	array[a] = array[b];
	array[b] = num;
}

var dots = 0;
var counter = 0;
var Xs = new Array();
var Ys = new Array();
var intersections = new Array();
var crap = 0;
var whichPoints = 0;
var doIDraw = false;
var lineNum = 2;

function addLine(){
	lineNum++;
	document.getElementById('lineNum').innerHTML = "Number of possible lines is " + lineNum;
}

function deleteLine(){
	lineNum--;
	document.getElementById('lineNum').innerHTML = "Number of possible lines is " + lineNum;
	for(var i = 0; i < 4; i++){
		Xs[lineNum*4-(i+1)];
		Ys[lineNum*4-(i+1)];
	}
}

function removeStuff(){
	if(document.getElementById('grid').checked == false){
		document.getElementById('stuff').style.visibility="hidden";
	}

	else{
		document.getElementById('stuff').style.visibility="visible";
	}
	draw(Xs,Ys,null);
}

for(var i = 0; i < 21; i++){
	for (var j = 0; j < 21; j++){
		intersections[crap] = 60 + (i*30);
		crap++;
		intersections[crap] = 650 - (j*30);
		crap++;
	}
}

function sort(){
	for(var i = 0; i < Xs.length; i++){
		var bestPos = i;
		for(var j = i+1; j < Xs.length;j++){
			if(Xs[j] < Xs[bestPos]){
				bestPos = j;
			}
		}
		swap(Xs,bestPos,i);
		swap(Ys,bestPos,i);
	}
}


function clear(){
	var c = document.getElementById('canvas');
	var ctx = c.getContext('2d');
	ctx.clearRect(0,0,1500,1000);
}

function change(){
	document.getElementById('Left').innerHTML = document.getElementById('Y text').value;
	document.getElementById('Bottom').innerHTML = document.getElementById('X text').value;
	if(document.getElementById('Y text').value == ''){
		xOffset = -11;
	}

	else{
		xOffset = -31;
	}
	draw();
}

function draw(scale){


	var c = document.getElementById('canvas');
	var ctx = c.getContext('2d');

	clear();
	ctx.beginPath();
	ctx.moveTo(160,0);
	ctx.lineTo(160,600)
	ctx.lineWidth = 2;
	ctx.stroke();
	ctx.beginPath();
	ctx.moveTo(140,580);
	ctx.lineTo(755,580);
	ctx.stroke();

	drawBezier(Xs[0],Ys[0],Xs[1],Ys[1],Xs[2],Ys[2],Xs[3],Ys[3],'#35f4ef',document.getElementById('dots').checked);
	drawBezier(Xs[4],Ys[4],Xs[5],Ys[5],Xs[6],Ys[6],Xs[7],Ys[7],'#f434da',document.getElementById('dots').checked);

	if(document.getElementById('grid').checked){

		
		ctx.lineWidth = 0.25;

		

		if(document.getElementById('YInterval').value != '' && document.getElementById('EndY').value != '' && document.getElementById('YInterval').value != '0' && document.getElementById('EndY').value != '0'){
			var yChange = ((document.getElementById('EndY').value - document.getElementById('StartY').value) / document.getElementById('YInterval').value) ;
			for(var i = 0; i < yChange;i++){
	
				ctx.beginPath();
				ctx.moveTo(735,(580 - ((i+1)*(580/yChange))));
				ctx.lineTo(160,(580 - ((i+1)*(580/yChange))));
				ctx.stroke();
				ctx.fillStyle = "black";
				ctx.font = "15px Calisto MT";
				ctx.fillText(document.getElementById('StartY').value + document.getElementById('YInterval').value * (i+1),120,580-(i+1)*(580/yChange));
			}
		}

		if(document.getElementById('XInterval').value != '' && document.getElementById('EndX').value != '' && document.getElementById('XInterval').value != '0' && document.getElementById('EndX').value != '0'){

			var xChange = ((document.getElementById('EndX').value - document.getElementById('StartX').value) / document.getElementById('XInterval').value);

			for(var i = 0; i < xChange;i++){
				ctx.beginPath();
				ctx.moveTo((160 + ((i+1)*(560/xChange))),580);
				ctx.lineTo((160 + ((i+1)*(560/xChange))),8);
				ctx.stroke();
				ctx.fillStyle = "black";
				ctx.font = "15px Calisto MT";
				ctx.fillText('' + document.getElementById('StartX').value + document.getElementById('XInterval').value * (i+1),160 + ((i+1)*(560/xChange)), 600);

			}
		}
	
	}
}


draw(null);

function mouseDown(e){

	

	if(document.getElementById('dots').checked){

		doIDraw = true;

		if(counter > (lineNum*4)-1) {
			doIDraw = false;
			return null;
		}

		for (var i = 0; i < Xs.length; i++){
			if(withinCircle(Xs[i],Ys[i],e.clientX+xOffset,e.clientY+yOffset,10)){
				doIDraw = true;
				whichPoints = i;
				return null;
			}
		}
	
		if(document.getElementById('snap').checked){
			//*
			var bestPos = 0;
			for(var i = 2; i < intersections.length; i+=2){
				var x = Math.abs(intersections[i] - (e.clientX+xOffset));
				var y = Math.abs(intersections[i+1] - (e.clientY+yOffset));
				var bestX = Math.abs(intersections[bestPos] - (e.clientX+xOffset));
				var bestY = Math.abs(intersections[bestPos+1] - (e.clientY+yOffset));
				if(x<=bestX && y<=bestY){
					bestPos=i;
				}
			}
	
			dots++;
			Xs[counter] = intersections[bestPos];
			Ys[counter] = intersections[bestPos+1];
			/*
			Xs[counter] = Math.round((e.clientX-11)/30) * 30;
			Ys[counter] = Math.round((e.clientY-30)/30) * 30 + 20;
			//*/
	
	
		}
		else{
			dots++;
			Xs[counter] = e.clientX+xOffset;
			Ys[counter] = e.clientY+yOffset;
		}
		if(counter >=3){
			drawBezier(Xs[0],Ys[0],Xs[1],Ys[1],Xs[2],Ys[2],Xs[3],Ys[3]);
		}
		counter ++;
	}
	draw(null);
}
function mouseUp(){
	doIDraw = false;
}

function mouseMove(e){
	if(doIDraw){
		Xs[whichPoints] = e.clientX+xOffset;
		Ys[whichPoints] = e.clientY+yOffset;
		draw(null);
	}
}
