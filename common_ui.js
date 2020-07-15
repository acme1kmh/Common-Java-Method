// 로딩 레이어 - 박일수 20190620
var VrLoading = (function(){
	var _this = {};
	_this.show = function(){
		$('#container').oLoader({
			  wholeWindow: true,
			  lockOverflow: false,
			  backgroundColor: '#000',
			  fadeInTime: 1000,
			  fadeLevel: 0.4,
			  image: '/resources/images/admin/loading.gif'
//			  hideAfter: 1500
		});
	}
	_this.hide = function() {
		$('#container').oLoader('hide');
	}
	return _this;
})();

(function($) {
    $.ajaxSetup({
        beforeSend: function(xhr) {
        	xhr.setRequestHeader("AJAX", true);
        },error: function(xhr, status, err) {
	        if (xhr.status == 401) {
	            alert("401")
	        } else if (xhr.status == 403) {
	        	alert(ERRSES27);
	        	location.href = "/admin/login.do";
	        } else {
	            alert(ERRPRU6(xhr.status));
	        }
	    }
//        timeout: 300000 //300 second (5minute)
    });

})(jQuery);


$(document).ready(function(){
	setLangSession(sessionStorage.getItem("lang"));
	$.datepicker.setDefaults($.datepicker.regional[sessionStorage.getItem("lang")]);

	$.i18n.properties({
		name: 'messages_' + sessionStorage.getItem("lang")
		, path: '/properties/'
		, mode: 'both'
		, language: 'l'
		, callback: function(){}
	});

	$(document).ajaxStart(function(){
		if($("#vodMgtDiv").length > 0){
			$(".oloader_canvas").show();
			$.oPageLoader({
				wholeWindow: true,
				lockOverflow: false,
				backgroundColor: '#000',
				fadeInTime: 300,
				fadeLevel: 0.4
			});
		}else{
			//로딩바 셋팅
			$('#container').oLoader({
				wholeWindow: true,
				lockOverflow: false,
				backgroundColor: '#000',
				fadeInTime: 300,
				fadeLevel: 0.4,
				image: '/resources/images/admin/loading.gif'
//						  hideAfter: 1500
			});
		}
	}).ajaxStop(function() {
		if($("#vodMgtDiv").length > 0){
//			$(".oloader_canvas").hide();
		}else{
			$('#container').oLoader('hide');
		}
	});

	$('.btn-group .btn').bind('click', function(){
		$(this).addClass('on').siblings().removeClass('on');
	});
	$('.main-menu .dropdown a').unbind('click').bind('click', function(){
		$(this).parent('li').toggleClass('current').siblings().removeClass('current');
	});
	var selectTarget = $('.selectbox select');
	selectTarget.on({
		'focus': function() {
			$(this).parent().addClass('focus');
		},
		'blur': function() {
			$(this).parent().removeClass('focus');
		}
	});
	selectTarget.change(function() {
		var select_name = $(this).children('option:selected').text();
		$(this).siblings('label').text(select_name);
		$(this).parent().removeClass('focus');
	});


});

function setLangSession(lang){
	//console.log(lang + " | " + sessionStorage.getItem("lang"));
	if(lang){
		if(lang != sessionStorage.getItem("lang")){
			sessionStorage.removeItem("lang");
			sessionStorage.setItem("lang", lang);
			$.ajax({
				url: getContextPath() + "/setSession.do"
				, data: {sendLang : lang}
				, dataType: 'json'
				, type: 'post'
				, success: function(data){
					location.reload();
				}
			});
		}
	}else{
		sessionStorage.setItem("lang", "en");
	}
}

var layerPopup = (function() {
	var html = undefined;
    return {
		"show" : function(elem, container) {
			elem = document.querySelector(elem);
			if (container == null){
				html = (document.body.scrollTop == '0') ? document.documentElement : document.body;
			} else {
				html = container;
			}
			this.top = html.scrollTop;
			html.style.top = (0 - this.top) + "px";
			html.classList.add('noscroll')
			elem.classList.add('visible');
		},
		"hide" : function(elem) {
			elem = document.querySelector(elem);
			html.classList.remove("noscroll");
			html.scrollTop = this.top;
			html.style.top = "";
			elem.classList.remove('visible');
		}
    };
}());

//메뉴 셋팅
function selectedMenu(dropdownIdx, dropdownMenuIdx) {
	$(".dropdown").removeClass("current");
	$(".dropdown:eq(" + dropdownIdx + ")").addClass("current");

	$(".ajax-link").removeClass("active")
	$(".dropdown:eq(" + dropdownIdx + ")>.dropdown-menu>li:eq(" + dropdownMenuIdx + ")>a").addClass("active");
}

function getContextPath(){
	var hostIndex = location.href.indexOf(location.host) + location.host.length;
	return location.href.substring(hostIndex, location.href.indexOf("/", hostIndex + 1));
}

browser = (function(){
	var a = navigator.userAgent.toLowerCase();
	var b, v;

	if (a.indexOf("safari/") > -1) {
		b = "safari";
		var s = a.indexOf("version/");
		var l = a.indexOf(" ", s);
		v = a.substring(s + 8, l);
	}
	if (a.indexOf("chrome/") > -1) {
		b = "chrome";
		var ver = /[ \/]([\w.]+)/.exec(a) || [];
		v = ver[1];
	}
	if (a.indexOf("firefox/") > -1) {
		b = "firefox";
		var ver = /(?:.*? rv:([\w.]+)|)/.exec(a) || [];
		v = ver[1];
	}
	if (a.indexOf("opera/") > -1) {
		b = "opera";
		var ver = /(?:.*version|)[ \/]([\w.]+)/.exec(a) || [];
		v = ver[1];
	}
	if ((a.indexOf("msie") > -1) || (a.indexOf(".net") > -1)) {
		b = "msie";
		var ver = /(?:.*? rv:([\w.]+))?/.exec(a) || [];
		if (ver[1])
			v = ver[1];
		else {
			var s = a.indexOf("msie");
			var l = a.indexOf(".", s);
			v = a.substring(s + 4, l);
		}
	}
	return {
		name : b || "",
		version : v || 0
	};
}());

function mapElements(value, key, map){
	//console.log("m[" + key + "] = " + value);
	formData.append(key, value);
}

//Input item byte limit
function inputByteCheck(id, maxByte) {
	console.log("input");
	var str = $("#" + id).val();
	for(b = i = 0;c = str.charCodeAt(i);) {
		b += c >> 7?2:1;
		if (b > maxByte) {
			break;
		}
		i++;
	}
	$("#" + id).val(str.substring(0,i));
}

function dateFormat(date){

	var year = date.substring(0,4);
	var month = date.substring(4,6);
	var day = date.substring(6,8);

	var comDate = new Date(year, month-1, day);
	var lang = sessionStorage.getItem("lang");

	if(date.length > 6){
		if(lang == "ko"){
			return $.datepicker.formatDate("yy-mm-dd",comDate);
		}else{
			return $.datepicker.formatDate("dd-mm-yy",comDate);
		}
	}else{
		if(lang == "ko"){
			return $.datepicker.formatDate("yy-mm",comDate);
		}else{
			return $.datepicker.formatDate("mm-yy",comDate);
		}
	}
}

String.prototype.escapeHtml = function(){
  return this.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/\"/g, "&quot;").replace(/\'/g, "&#039;");
};

String.prototype.unescapeHtml = function(){
  return this.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, "\"").replace(/&#39;/g, "\'");
};


