<script lang="ts">
	import { enhance } from '$app/forms';

	// properties of component taken from instantiation

	function addUser() {
		const users = document.getElementById('unames');
		if (users) {
			const row = document.createElement('div');
			const n = users.childElementCount + 1;
			row.className = 'row';
			row.id = `div${n}`;
			row.innerHTML = `
           <label for="user${n}">
               Friend ${n}:
               <input id="user${n}" name="user${n}" type="text" placeholder="Input StoryGraph username here"/>
           </label>
           <button type="button" id=button${n}>-</button>
         `;
			users.appendChild(row);
			const button = document.getElementById(`button${n}`);
			button?.addEventListener('click', function (e: Event) {
				const id = this.id.replace('button', '');
				removeUser(+id);
			});
		}
	}

	function removeUser(id: number) {
		const numUsers = document.getElementById('unames')?.childElementCount;
		document.getElementById(`div${id}`)?.remove();

		if (numUsers && numUsers > id) {
			for (let i = id + 1; i <= numUsers; i++) {
				let divN = document.getElementById(`div${i}`);
				let textN = (<HTMLInputElement>document.getElementById(`user${i}`)).value;
				if (divN) {
					console.log(`divNum: ${i}`);
					divN.id = `div${i - 1}`;
					divN.innerHTML = `
					  <label for="user${i - 1}">
                             Friend ${i - 1}:
                             <input id="user${i - 1}" name="user${i - 1}" type="text" placeholder="Input StoryGraph username here"/>
                         </label>
                         <button type="button" id=button${i - 1}>-</button>
                       `;

					let inputN = <HTMLInputElement>document.getElementById(`user${i - 1}`);
					console.log(`Next Text: ${textN}`);
					inputN.setAttribute('value', textN);

					// reattach event listener
					const newButton = divN.querySelector('button');
					newButton?.addEventListener('click', function () {
						removeUser(+(this as HTMLButtonElement).id.replace('button', ''));
					});
				}
			}
		}
	}
</script>

<div id="title-block">
	<h1 id="title">want ideas for your next book club book?</h1>
</div>

<div id="form">
	<form method="POST" name="unamesForm" use:enhance>
		<div id="unames">
			<div class="row" id="div1">
				<label for="user1">
					Friend 1:
					<input
						required
						id="user1"
						name="user1"
						type="text"
						placeholder="Input StoryGraph username here"
					/>
				</label>
			</div>

			<div class="row" id="div2">
				<label for="user2">
					Friend 2:
					<input
						required
						id="user2"
						name="user2"
						type="text"
						placeholder="Input StoryGraph username here"
					/>
				</label>
			</div>
		</div>

		<div class="row">
			Add row
			<button type="button" onclick={addUser}>Add user</button>
		</div>

		<div class="row">
			<label>
				<button type="submit">Submit</button>
			</label>
		</div>
	</form>
</div>

<style>
	#title-block {
		display: flex;
		justify-content: center;
		align-items: flex-end;
		height: 35vh;
		margin-bottom: 1vh;
	}

	#title {
		font-weight: bold;
		font-size: 2rem;
		font-family:
			Atkinson Hyperlegible,
			sans-serif;
		max-width: 70vw;
		text-align: center;
	}

	#form {
		display: flex;
		justify-content: center;
		font-size: 1.5rem;
		width: 100%;
	}

	form {
		min-width: 50vw;
	}
	input {
		display: inline-block;
		width: 35vw;
		height: 50px;
		margin-bottom: 10px;
	}
</style>
